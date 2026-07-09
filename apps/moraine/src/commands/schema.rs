use anyhow::{bail, Result};
use serde::Serialize;
use serde_json::json;

use crate::cli::SchemaAnalyticsArgs;

pub(super) const EVENTS_SCHEMA_VERSION: &str = "moraine.analytics.events.v1";
pub(super) const EXPORT_METADATA_SCHEMA_VERSION: &str = "moraine.analytics.export_metadata.v1";

pub(super) const DEFAULT_EVENT_COLUMNS: &[&str] = &[
    "session_id",
    "event_uid",
    "event_ts",
    "turn_seq",
    "event_order",
    "harness",
    "source_name",
    "event_kind",
    "payload_type",
    "actor_kind",
    "tool_name",
    "tool_phase",
    "tool_error",
    "model",
    "project_id",
];

#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct EventColumn {
    pub name: &'static str,
    pub source_expression: &'static str,
    #[serde(skip)]
    pub select_expression: &'static str,
    #[serde(rename = "type")]
    pub value_type: &'static str,
    pub default: bool,
    pub sensitive: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct EventFilter {
    name: &'static str,
    flag: &'static str,
    arity: &'static str,
    source: &'static str,
    empty_value: &'static str,
}

const fn col(
    name: &'static str,
    source_expression: &'static str,
    select_expression: &'static str,
    value_type: &'static str,
    default: bool,
    sensitive: bool,
) -> EventColumn {
    EventColumn {
        name,
        source_expression,
        select_expression,
        value_type,
        default,
        sensitive,
    }
}

pub(super) const EVENT_COLUMNS: &[EventColumn] = &[
    col(
        "session_id",
        "e.session_id",
        "e.session_id",
        "string",
        true,
        false,
    ),
    col(
        "event_uid",
        "e.event_uid",
        "e.event_uid",
        "string",
        true,
        false,
    ),
    col("author", "e.author", "e.author", "string", false, false),
    col(
        "event_ts",
        "CLI format of e.event_unix_ms",
        "e.event_unix_ms",
        "string",
        true,
        false,
    ),
    col(
        "event_unix_ms",
        "e.event_unix_ms",
        "e.event_unix_ms",
        "integer",
        false,
        false,
    ),
    col(
        "event_order",
        "e.event_order",
        "e.event_order",
        "integer",
        true,
        false,
    ),
    col(
        "turn_seq",
        "e.turn_seq",
        "e.turn_seq",
        "integer",
        true,
        false,
    ),
    col("harness", "e.harness", "e.harness", "string", true, false),
    col(
        "inference_provider",
        "e.inference_provider",
        "e.inference_provider",
        "string",
        false,
        false,
    ),
    col(
        "source_name",
        "e.source_name",
        "e.source_name",
        "string",
        true,
        false,
    ),
    col(
        "source_generation",
        "e.source_generation",
        "e.source_generation",
        "integer",
        false,
        false,
    ),
    col(
        "source_line_no",
        "e.source_line_no",
        "e.source_line_no",
        "integer",
        false,
        false,
    ),
    col(
        "source_offset",
        "e.source_offset",
        "e.source_offset",
        "integer",
        false,
        false,
    ),
    col(
        "source_file",
        "e.source_file",
        "e.source_file",
        "string",
        false,
        true,
    ),
    col(
        "source_ref",
        "e.source_ref",
        "e.source_ref",
        "string",
        false,
        true,
    ),
    col(
        "event_kind",
        "e.event_kind",
        "e.event_kind",
        "string",
        true,
        false,
    ),
    col(
        "payload_type",
        "e.payload_type",
        "e.payload_type",
        "string",
        true,
        false,
    ),
    col(
        "actor_kind",
        "e.actor_kind",
        "e.actor_kind",
        "string",
        true,
        false,
    ),
    col("op_kind", "e.op_kind", "e.op_kind", "string", false, false),
    col(
        "op_status",
        "e.op_status",
        "e.op_status",
        "string",
        false,
        false,
    ),
    col(
        "request_id",
        "e.request_id",
        "e.request_id",
        "string",
        false,
        false,
    ),
    col(
        "trace_id",
        "e.trace_id",
        "e.trace_id",
        "string",
        false,
        false,
    ),
    col("item_id", "e.item_id", "e.item_id", "string", false, false),
    col(
        "tool_call_id",
        "e.tool_call_id",
        "e.tool_call_id",
        "string",
        false,
        false,
    ),
    col(
        "parent_tool_call_id",
        "e.parent_tool_call_id",
        "e.parent_tool_call_id",
        "string",
        false,
        false,
    ),
    col(
        "tool_name",
        "e.tool_name",
        "e.tool_name",
        "string",
        true,
        false,
    ),
    col(
        "tool_phase",
        "e.tool_phase",
        "e.tool_phase",
        "string",
        true,
        false,
    ),
    col(
        "tool_error",
        "e.tool_error != 0",
        "e.tool_error != 0",
        "boolean",
        true,
        false,
    ),
    col(
        "agent_run_id",
        "e.agent_run_id",
        "e.agent_run_id",
        "string",
        false,
        false,
    ),
    col(
        "agent_label",
        "e.agent_label",
        "e.agent_label",
        "string",
        false,
        false,
    ),
    col(
        "coord_group_id",
        "e.coord_group_id",
        "e.coord_group_id",
        "string",
        false,
        false,
    ),
    col(
        "coord_group_label",
        "e.coord_group_label",
        "e.coord_group_label",
        "string",
        false,
        false,
    ),
    col(
        "is_substream",
        "e.is_substream != 0",
        "e.is_substream != 0",
        "boolean",
        false,
        false,
    ),
    col("model", "e.model", "e.model", "string", true, false),
    col(
        "endpoint_kind",
        "e.endpoint_kind",
        "e.endpoint_kind",
        "string",
        false,
        false,
    ),
    col(
        "input_tokens",
        "e.input_tokens",
        "e.input_tokens",
        "integer",
        false,
        false,
    ),
    col(
        "output_tokens",
        "e.output_tokens",
        "e.output_tokens",
        "integer",
        false,
        false,
    ),
    col(
        "cache_read_tokens",
        "e.cache_read_tokens",
        "e.cache_read_tokens",
        "integer",
        false,
        false,
    ),
    col(
        "cache_write_tokens",
        "e.cache_write_tokens",
        "e.cache_write_tokens",
        "integer",
        false,
        false,
    ),
    col(
        "latency_ms",
        "e.latency_ms",
        "e.latency_ms",
        "integer",
        false,
        false,
    ),
    col(
        "retry_count",
        "e.retry_count",
        "e.retry_count",
        "integer",
        false,
        false,
    ),
    col(
        "service_tier",
        "e.service_tier",
        "e.service_tier",
        "string",
        false,
        false,
    ),
    col(
        "content_types",
        "e.content_types",
        "e.content_types",
        "array<string>",
        false,
        false,
    ),
    col(
        "has_reasoning",
        "e.has_reasoning != 0",
        "e.has_reasoning != 0",
        "boolean",
        false,
        false,
    ),
    col(
        "project_id",
        "e.project_id",
        "e.project_id",
        "string",
        true,
        false,
    ),
    col(
        "repo_rel_path",
        "e.repo_rel_path",
        "e.repo_rel_path",
        "string",
        false,
        true,
    ),
    col(
        "worktree_root",
        "e.worktree_root",
        "e.worktree_root",
        "string",
        false,
        true,
    ),
    col("cwd", "e.cwd", "e.cwd", "string", false, true),
    col(
        "text_preview",
        "e.text_preview",
        "e.text_preview",
        "string",
        false,
        true,
    ),
    col(
        "text_content",
        "e.text_content",
        "e.text_content",
        "string",
        false,
        true,
    ),
    col(
        "payload_json",
        "e.payload_json",
        "e.payload_json",
        "JSON string",
        false,
        true,
    ),
    col(
        "token_usage_json",
        "e.token_usage_json",
        "e.token_usage_json",
        "JSON string",
        false,
        true,
    ),
    col(
        "token_usage_buckets",
        "e.token_usage_buckets",
        "e.token_usage_buckets",
        "object",
        false,
        false,
    ),
    col(
        "token_usage_native_units",
        "e.token_usage_native_units",
        "e.token_usage_native_units",
        "object",
        false,
        false,
    ),
    col(
        "event_version",
        "e.event_version",
        "e.event_version",
        "integer",
        false,
        false,
    ),
];

const EVENT_FILTERS: &[EventFilter] = &[
    EventFilter {
        name: "since",
        flag: "--since",
        arity: "single",
        source: "canonical event time >= value",
        empty_value: "invalid timestamp fails",
    },
    EventFilter {
        name: "until",
        flag: "--until",
        arity: "single",
        source: "canonical event time < value",
        empty_value: "invalid timestamp fails",
    },
    EventFilter {
        name: "session_id",
        flag: "--session-id",
        arity: "repeatable",
        source: "session_id = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "harness",
        flag: "--harness",
        arity: "repeatable",
        source: "harness = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "source_name",
        flag: "--source-name",
        arity: "repeatable",
        source: "source_name = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "project_id",
        flag: "--project-id",
        arity: "repeatable",
        source: "project_id = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "cwd_prefix",
        flag: "--cwd-prefix",
        arity: "repeatable",
        source: "cwd = value OR startsWith(cwd, value + \"/\")",
        empty_value: "rejected",
    },
    EventFilter {
        name: "worktree_root",
        flag: "--worktree-root",
        arity: "repeatable",
        source: "worktree_root = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "repo_rel_path",
        flag: "--repo-rel-path",
        arity: "repeatable",
        source: "repo_rel_path = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "event_kind",
        flag: "--event-kind",
        arity: "repeatable",
        source: "event_kind = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "payload_type",
        flag: "--payload-type",
        arity: "repeatable",
        source: "payload_type = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "actor_kind",
        flag: "--actor-kind",
        arity: "repeatable",
        source: "actor_kind = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "model_name",
        flag: "--model-name",
        arity: "repeatable",
        source: "model = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "tool_name",
        flag: "--tool-name",
        arity: "repeatable",
        source: "tool_name = value",
        empty_value: "rejected",
    },
    EventFilter {
        name: "tool_error_only",
        flag: "--tool-error-only",
        arity: "single boolean",
        source: "tool_error = 1",
        empty_value: "not applicable",
    },
];

pub(super) fn event_column(name: &str) -> Option<&'static EventColumn> {
    EVENT_COLUMNS.iter().find(|column| column.name == name)
}

pub(super) fn default_event_columns() -> Vec<&'static EventColumn> {
    DEFAULT_EVENT_COLUMNS
        .iter()
        .map(|name| event_column(name).expect("default event column must be in public schema"))
        .collect()
}

pub(super) fn all_non_sensitive_event_columns() -> Vec<&'static EventColumn> {
    EVENT_COLUMNS
        .iter()
        .filter(|column| !column.sensitive)
        .collect()
}

pub(crate) fn render_analytics(args: &SchemaAnalyticsArgs) -> Result<()> {
    if !args.json {
        bail!("moraine schema analytics currently requires --json");
    }

    println!("{}", serde_json::to_string_pretty(&analytics_schema())?);
    Ok(())
}

fn analytics_schema() -> serde_json::Value {
    let default_columns = DEFAULT_EVENT_COLUMNS.to_vec();
    let sensitive_columns = EVENT_COLUMNS
        .iter()
        .filter(|column| column.sensitive)
        .map(|column| column.name)
        .collect::<Vec<_>>();

    json!({
        "schema_version": "moraine.analytics.schema.v1",
        "data_schema_version": EVENTS_SCHEMA_VERSION,
        "export_kind": "events",
        "formats": ["jsonl"],
        "default_columns": default_columns,
        "sensitive_columns": sensitive_columns,
        "columns": EVENT_COLUMNS,
        "filters": EVENT_FILTERS,
        "timestamp_semantics": {
            "event_ts": "UTC RFC3339 string with millisecond precision",
            "source": "ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at)",
            "since": "inclusive",
            "until": "exclusive"
        },
        "column_semantics": {
            "strings": "unknown or unavailable values are emitted as empty strings",
            "numbers": "unknown numeric values follow storage defaults",
            "booleans": "emitted as JSON booleans"
        },
        "metadata_schema_version": EXPORT_METADATA_SCHEMA_VERSION,
        "metadata_target": "stderr",
        "scope": {
            "included": ["events"],
            "excluded": ["sessions", "turns", "tool-io", "raw-events", "csv", "http", "mcp-batch-open"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANALYTICS_SCHEMA_GOLDEN: &str = include_str!("analytics_schema_v1.golden.json");

    #[test]
    fn analytics_schema_json_matches_golden() {
        let rendered = serde_json::to_string_pretty(&analytics_schema()).expect("render schema");
        assert_eq!(rendered, ANALYTICS_SCHEMA_GOLDEN.trim_end());
    }

    #[test]
    fn analytics_schema_has_expected_versions_and_defaults() {
        let schema = analytics_schema();
        assert_eq!(schema["schema_version"], "moraine.analytics.schema.v1");
        assert_eq!(schema["data_schema_version"], EVENTS_SCHEMA_VERSION);
        assert_eq!(
            schema["default_columns"]
                .as_array()
                .expect("default columns"),
            &DEFAULT_EVENT_COLUMNS
                .iter()
                .map(|name| json!(name))
                .collect::<Vec<_>>()
        );
        assert!(schema["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .any(|column| column["name"] == "event_ts"
                && column["type"] == "string"
                && column["default"] == true
                && column["sensitive"] == false));
    }

    #[test]
    fn sensitive_column_set_matches_contract() {
        let sensitive = EVENT_COLUMNS
            .iter()
            .filter(|column| column.sensitive)
            .map(|column| column.name)
            .collect::<Vec<_>>();

        assert_eq!(
            sensitive,
            vec![
                "source_file",
                "source_ref",
                "repo_rel_path",
                "worktree_root",
                "cwd",
                "text_preview",
                "text_content",
                "payload_json",
                "token_usage_json",
            ]
        );
    }

    #[test]
    fn default_columns_are_public_and_non_sensitive() {
        for column in default_event_columns() {
            assert!(column.default, "{} should be marked default", column.name);
            assert!(!column.sensitive, "{} must not be sensitive", column.name);
        }
    }
}
