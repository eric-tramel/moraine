use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDateTime, Utc};
use moraine_conversations::{
    ConversationMode, ConversationSummary, McpEventRef, McpEventSummary, SessionMetadata,
    TraceEvent, TurnSummary,
};
use serde_json::Value;

#[derive(Clone)]
pub(crate) struct StoredEvent {
    pub row: Value,
    pub trace: TraceEvent,
    pub source_name: String,
    pub harness: String,
    pub inference_provider: String,
    pub model: String,
}

impl StoredEvent {
    pub fn from_row(row: Value) -> Self {
        let event_time = string(&row, "event_ts");
        let trace = TraceEvent {
            session_id: string(&row, "session_id"),
            event_uid: string(&row, "event_uid"),
            event_order: event_order(&row),
            turn_seq: number(&row, "turn_index") as u32,
            event_time: event_time.clone(),
            event_unix_ms: timestamp_ms(&event_time),
            actor_role: string(&row, "actor_kind"),
            event_class: string(&row, "event_kind"),
            payload_type: string(&row, "payload_type"),
            call_id: string(&row, "tool_call_id"),
            name: string(&row, "tool_name"),
            phase: string(&row, "tool_phase"),
            item_id: string(&row, "item_id"),
            source_ref: string(&row, "source_ref"),
            text_content: string(&row, "text_content"),
            payload_json: string(&row, "payload_json"),
            token_usage_json: string(&row, "token_usage_json"),
            endpoint_kind: string(&row, "endpoint_kind"),
            token_usage_buckets: u64_map(&row, "token_usage_buckets"),
            token_usage_native_units: f64_map(&row, "token_usage_native_units"),
        };
        Self {
            source_name: string(&row, "source_name"),
            harness: string(&row, "harness"),
            inference_provider: string(&row, "inference_provider"),
            model: string(&row, "model"),
            row,
            trace,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match (
            self.trace.event_class.as_str(),
            self.trace.actor_role.as_str(),
        ) {
            ("message" | "event_msg", "user") => "user_input",
            ("message" | "event_msg", "assistant") => "assistant_response",
            ("reasoning", _) => "reasoning",
            ("tool_call", _) => "tool_call",
            ("tool_result", _) => "tool_response",
            ("summary", _) => "compaction",
            (_, "system") | ("session_meta", _) => "system",
            ("runtime", _) => "runtime",
            _ => "unknown",
        }
    }

    pub fn event_ref(&self) -> McpEventRef {
        McpEventRef {
            session_id: self.trace.session_id.clone(),
            event_uid: self.trace.event_uid.clone(),
            event_order: self.trace.event_order,
            turn_seq: self.trace.turn_seq,
            event_time: self.trace.event_time.clone(),
            event_type: self.event_type().to_string(),
        }
    }

    pub fn event_summary(&self) -> McpEventSummary {
        McpEventSummary {
            session_id: self.trace.session_id.clone(),
            event_uid: self.trace.event_uid.clone(),
            event_order: self.trace.event_order,
            turn_seq: self.trace.turn_seq,
            event_time: self.trace.event_time.clone(),
            event_unix_ms: self.trace.event_unix_ms,
            actor_role: self.trace.actor_role.clone(),
            event_class: self.trace.event_class.clone(),
            payload_type: self.trace.payload_type.clone(),
            event_type: self.event_type().to_string(),
            call_id: self.trace.call_id.clone(),
            name: self.trace.name.clone(),
            phase: self.trace.phase.clone(),
            text_preview: non_empty(string(&self.row, "text_preview")),
        }
    }
}

pub(crate) fn conversation_summary(events: &[StoredEvent]) -> Option<ConversationSummary> {
    let first = events.first()?;
    let last = events.last()?;
    let turns = events
        .iter()
        .map(|event| event.trace.turn_seq)
        .collect::<BTreeSet<_>>()
        .len() as u32;
    let count = |class: &str, actor: Option<&str>| {
        events
            .iter()
            .filter(|event| {
                event.trace.event_class == class
                    && actor.is_none_or(|role| event.trace.actor_role == role)
            })
            .count() as u64
    };
    let meta = events
        .iter()
        .rev()
        .find(|event| event.trace.event_class == "session_meta");
    Some(ConversationSummary {
        session_id: first.trace.session_id.clone(),
        first_event_time: first.trace.event_time.clone(),
        first_event_unix_ms: first.trace.event_unix_ms,
        last_event_time: last.trace.event_time.clone(),
        last_event_unix_ms: last.trace.event_unix_ms,
        total_turns: turns,
        total_events: events.len() as u64,
        user_messages: count("message", Some("user")) + count("event_msg", Some("user")),
        assistant_messages: count("message", Some("assistant"))
            + count("event_msg", Some("assistant")),
        tool_calls: count("tool_call", None),
        tool_results: count("tool_result", None),
        mode: mode(events),
        session_slug: meta.and_then(|event| payload_field(event, "slug")),
        session_summary: meta.and_then(|event| {
            payload_field(event, "summary")
                .or_else(|| payload_field(event, "title"))
                .or_else(|| payload_field(event, "name"))
        }),
    })
}

pub(crate) fn session_metadata(events: &[StoredEvent]) -> Option<SessionMetadata> {
    let summary = conversation_summary(events)?;
    let first = events.first()?;
    let last = events.last()?;
    Some(SessionMetadata {
        session_id: summary.session_id,
        first_event_time: summary.first_event_time,
        first_event_unix_ms: summary.first_event_unix_ms,
        last_event_time: summary.last_event_time,
        last_event_unix_ms: summary.last_event_unix_ms,
        total_turns: summary.total_turns,
        total_events: summary.total_events,
        user_messages: summary.user_messages,
        assistant_messages: summary.assistant_messages,
        tool_calls: summary.tool_calls,
        tool_results: summary.tool_results,
        mode: summary.mode,
        first_event_uid: first.trace.event_uid.clone(),
        last_event_uid: last.trace.event_uid.clone(),
        last_actor_role: last.trace.actor_role.clone(),
    })
}

pub(crate) fn turn_summary(events: &[StoredEvent]) -> Option<TurnSummary> {
    let first = events.first()?;
    let last = events.last()?;
    let count = |class: &str, actor: Option<&str>| {
        events
            .iter()
            .filter(|event| {
                event.trace.event_class == class
                    && actor.is_none_or(|role| event.trace.actor_role == role)
            })
            .count() as u64
    };
    Some(TurnSummary {
        session_id: first.trace.session_id.clone(),
        turn_seq: first.trace.turn_seq,
        turn_id: string(&first.row, "turn_id"),
        started_at: first.trace.event_time.clone(),
        started_at_unix_ms: first.trace.event_unix_ms,
        ended_at: last.trace.event_time.clone(),
        ended_at_unix_ms: last.trace.event_unix_ms,
        total_events: events.len() as u64,
        user_messages: count("message", Some("user")) + count("event_msg", Some("user")),
        assistant_messages: count("message", Some("assistant"))
            + count("event_msg", Some("assistant")),
        tool_calls: count("tool_call", None),
        tool_results: count("tool_result", None),
        reasoning_items: count("reasoning", None),
    })
}

pub(crate) fn group_sessions(events: Vec<StoredEvent>) -> BTreeMap<String, Vec<StoredEvent>> {
    let mut grouped = BTreeMap::new();
    for event in events {
        grouped
            .entry(event.trace.session_id.clone())
            .or_insert_with(Vec::new)
            .push(event);
    }
    grouped
}

pub(crate) fn group_turns(events: &[StoredEvent]) -> BTreeMap<u32, Vec<StoredEvent>> {
    let mut grouped = BTreeMap::new();
    for event in events {
        grouped
            .entry(event.trace.turn_seq)
            .or_insert_with(Vec::new)
            .push(event.clone());
    }
    grouped
}

pub(crate) fn string(row: &Value, key: &str) -> String {
    row.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn number(row: &Value, key: &str) -> u64 {
    row.get(key).and_then(Value::as_u64).unwrap_or_default()
}

pub(crate) fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn event_order(row: &Value) -> u64 {
    number(row, "event_order").max(
        number(row, "source_line_no")
            .saturating_mul(1024)
            .saturating_add(number(row, "emission_index")),
    )
}

fn timestamp_ms(value: &str) -> i64 {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.timestamp_millis())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|time| time.and_utc().timestamp_millis())
        })
        .unwrap_or_else(|_| DateTime::<Utc>::UNIX_EPOCH.timestamp_millis())
}

fn u64_map(row: &Value, key: &str) -> BTreeMap<String, u64> {
    row.get(key)
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| value.as_u64().map(|value| (key.clone(), value)))
                .collect()
        })
        .unwrap_or_default()
}

fn f64_map(row: &Value, key: &str) -> BTreeMap<String, f64> {
    row.get(key)
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| value.as_f64().map(|value| (key.clone(), value)))
                .collect()
        })
        .unwrap_or_default()
}

fn payload_field(event: &StoredEvent, key: &str) -> Option<String> {
    serde_json::from_str::<Value>(&event.trace.payload_json)
        .ok()
        .as_ref()
        .map(|value| string(value, key))
        .and_then(non_empty)
}

fn mode(events: &[StoredEvent]) -> ConversationMode {
    if events.iter().any(|event| {
        event.trace.name.eq_ignore_ascii_case("web_search")
            || event.trace.payload_type.contains("web_search")
    }) {
        ConversationMode::WebSearch
    } else if events.iter().any(|event| {
        event.trace.name.to_ascii_lowercase().contains("moraine")
            && event.trace.event_class == "tool_call"
    }) {
        ConversationMode::McpInternal
    } else if events.iter().any(|event| {
        matches!(
            event.trace.event_class.as_str(),
            "tool_call" | "tool_result"
        )
    }) {
        ConversationMode::ToolCalling
    } else {
        ConversationMode::Chat
    }
}
