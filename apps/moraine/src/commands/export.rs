use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{ErrorKind, Write};
use std::process::ExitCode;
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::schema::{
    all_non_sensitive_event_columns, default_event_columns, event_column, EventColumn,
    EVENTS_SCHEMA_VERSION, EXPORT_METADATA_SCHEMA_VERSION,
};
use crate::cli::{ExportEventsArgs, ExportRowFormat};

const EXPORT_KIND_EVENTS: &str = "events";
const DEFAULT_BACKEND: &str = "default";
const DEFAULT_MAX_EXECUTION_SECONDS: u64 = 600;
const EXPORT_REQUEST_TIMEOUT_GRACE_SECONDS: u64 = 30;

pub(crate) async fn events(cfg: &AppConfig, args: ExportEventsArgs) -> Result<ExitCode> {
    let prepared = prepare_export(cfg, args)?;

    let client = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ensure_schema_ready(&client).await?;

    let started = Instant::now();
    let params = prepared
        .query_params
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let mut stream = client
        .request_stream_with_params(
            &prepared.query,
            Some(&cfg.clickhouse.database),
            None,
            &params,
            Some(request_timeout(cfg.clickhouse.timeout_seconds)),
        )
        .await?;

    let mut stdout = std::io::stdout().lock();
    let stream_result =
        stream_jsonl_rows(&mut stream, &mut stdout, &prepared.columns, prepared.limit).await?;

    if stream_result.broken_pipe {
        return Ok(ExitCode::SUCCESS);
    }

    let metadata = CompletionMetadata {
        schema_version: EXPORT_METADATA_SCHEMA_VERSION,
        data_schema_version: EVENTS_SCHEMA_VERSION,
        export_kind: EXPORT_KIND_EVENTS,
        backend: DEFAULT_BACKEND,
        query_id: &prepared.query_id,
        columns: prepared
            .columns
            .iter()
            .map(|column| column.name)
            .collect::<Vec<_>>(),
        filters: prepared.filters_metadata,
        limit: prepared.limit,
        row_count: stream_result.row_count,
        truncated: stream_result.truncated,
        elapsed_ms: started.elapsed().as_millis() as u64,
        sensitive_columns_requested: prepared.sensitive_columns_requested,
    };
    write_completion_metadata(&metadata)?;

    Ok(ExitCode::SUCCESS)
}

#[derive(Debug)]
struct PreparedExport {
    columns: Vec<&'static EventColumn>,
    sensitive_columns_requested: Vec<String>,
    limit: Option<usize>,
    query_id: String,
    query_params: Vec<(String, String)>,
    query: String,
    filters_metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct StreamRowsResult {
    row_count: usize,
    truncated: bool,
    broken_pipe: bool,
}

#[derive(Serialize)]
struct CompletionMetadata<'a> {
    schema_version: &'static str,
    data_schema_version: &'static str,
    export_kind: &'static str,
    backend: &'static str,
    query_id: &'a str,
    columns: Vec<&'static str>,
    filters: BTreeMap<String, Value>,
    limit: Option<usize>,
    row_count: usize,
    truncated: bool,
    elapsed_ms: u64,
    sensitive_columns_requested: Vec<String>,
}

fn prepare_export(cfg: &AppConfig, args: ExportEventsArgs) -> Result<PreparedExport> {
    validate_limit(args.limit)?;
    validate_format(args.format)?;
    let columns = select_columns(args.columns.as_deref(), args.include_sensitive)?;
    let sensitive_columns_requested = columns
        .iter()
        .filter(|column| column.sensitive)
        .map(|column| column.name.to_string())
        .collect::<Vec<_>>();
    let filters = EventFilters::from_args(&args)?;
    if !args.all && !filters.has_any_filter() {
        bail!("moraine export events requires at least one filter unless --all is supplied");
    }

    let query_id = Uuid::new_v4().to_string();
    let query = build_events_query(&cfg.clickhouse.database, &columns, &filters, args.limit)?;
    let query_params = build_query_params(&query_id);

    Ok(PreparedExport {
        columns,
        sensitive_columns_requested,
        limit: args.limit,
        query_id,
        query_params,
        query,
        filters_metadata: filters.metadata,
    })
}

fn validate_format(format: ExportRowFormat) -> Result<()> {
    match format {
        ExportRowFormat::Jsonl => Ok(()),
    }
}

fn validate_limit(limit: Option<usize>) -> Result<()> {
    if limit == Some(0) {
        bail!("--limit must be a positive integer");
    }
    Ok(())
}

fn select_columns(raw: Option<&str>, include_sensitive: bool) -> Result<Vec<&'static EventColumn>> {
    let columns = match raw {
        None => default_event_columns(),
        Some(value) if value.trim() == "all" => all_non_sensitive_event_columns(),
        Some(value) => {
            let mut seen = BTreeSet::new();
            let mut columns = Vec::new();
            for name in value.split(',') {
                let trimmed = name.trim();
                if trimmed.is_empty() {
                    bail!("--columns contains an empty column name");
                }
                let column = event_column(trimmed).ok_or_else(|| {
                    anyhow!(
                        "unsupported export column '{}'; run `moraine schema analytics --json` for the public column list",
                        trimmed
                    )
                })?;
                if !seen.insert(column.name) {
                    bail!("duplicate export column '{}'", column.name);
                }
                columns.push(column);
            }
            columns
        }
    };

    let sensitive = columns
        .iter()
        .filter(|column| column.sensitive)
        .map(|column| column.name)
        .collect::<Vec<_>>();
    if !include_sensitive && !sensitive.is_empty() {
        bail!(
            "sensitive export columns require --include-sensitive: {}",
            sensitive.join(", ")
        );
    }

    Ok(columns)
}

fn build_query_params(query_id: &str) -> Vec<(String, String)> {
    vec![
        ("query_id".to_string(), query_id.to_string()),
        ("readonly".to_string(), "1".to_string()),
        (
            "max_execution_time".to_string(),
            DEFAULT_MAX_EXECUTION_SECONDS.to_string(),
        ),
    ]
}

fn request_timeout(config_timeout_seconds: f64) -> Duration {
    let configured = config_timeout_seconds.max(1.0).ceil() as u64;
    let seconds = configured
        .max(DEFAULT_MAX_EXECUTION_SECONDS.saturating_add(EXPORT_REQUEST_TIMEOUT_GRACE_SECONDS));
    Duration::from_secs(seconds)
}

async fn ensure_schema_ready(client: &ClickHouseClient) -> Result<()> {
    let skew = client
        .schema_skew()
        .await
        .context("failed to check ClickHouse schema state before export")?;
    if skew.is_clean() {
        return Ok(());
    }

    let mut parts = Vec::new();
    if !skew.missing_on_server.is_empty() {
        parts.push(format!(
            "missing migrations on server: {}",
            skew.missing_on_server.join(", ")
        ));
    }
    if !skew.unknown_on_server.is_empty() {
        parts.push(format!(
            "unknown migrations on server: {}",
            skew.unknown_on_server.join(", ")
        ));
    }
    bail!(
        "ClickHouse schema is not compatible with this export contract ({}). Run `moraine db migrate` for the default backend or upgrade moraine if the server is newer.",
        parts.join("; ")
    );
}

#[derive(Debug)]
struct EventFilters {
    conditions: Vec<String>,
    metadata: BTreeMap<String, Value>,
    has_filter: bool,
}

impl EventFilters {
    fn from_args(args: &ExportEventsArgs) -> Result<Self> {
        let mut filters = Self {
            conditions: Vec::new(),
            metadata: BTreeMap::new(),
            has_filter: false,
        };

        let since = parse_optional_timestamp("--since", args.since.as_deref())?;
        let until = parse_optional_timestamp("--until", args.until.as_deref())?;
        if let (Some((since_raw, since_ms)), Some((until_raw, until_ms))) = (&since, &until) {
            if since_ms >= until_ms {
                bail!("--since must be earlier than --until");
            }
            filters.insert_metadata_string("since", since_raw.clone());
            filters.insert_metadata_string("until", until_raw.clone());
        } else {
            if let Some((raw, _)) = &since {
                filters.insert_metadata_string("since", raw.clone());
            }
            if let Some((raw, _)) = &until {
                filters.insert_metadata_string("until", raw.clone());
            }
        }

        if let Some((_, ms)) = since {
            filters.conditions.push(format!("e.event_unix_ms >= {ms}"));
            filters.has_filter = true;
        }
        if let Some((_, ms)) = until {
            filters.conditions.push(format!("e.event_unix_ms < {ms}"));
            filters.has_filter = true;
        }

        filters.add_exact("session_id", "session_id", &args.session_id)?;
        filters.add_exact("harness", "harness", &args.harness)?;
        filters.add_exact("source_name", "source_name", &args.source_name)?;
        filters.add_exact("project_id", "project_id", &args.project_id)?;
        filters.add_cwd_prefix(&args.cwd_prefix)?;
        filters.add_exact("worktree_root", "worktree_root", &args.worktree_root)?;
        filters.add_exact("repo_rel_path", "repo_rel_path", &args.repo_rel_path)?;
        filters.add_exact("event_kind", "event_kind", &args.event_kind)?;
        filters.add_exact("payload_type", "payload_type", &args.payload_type)?;
        filters.add_exact("actor_kind", "actor_kind", &args.actor_kind)?;
        filters.add_exact("model_name", "model", &args.model_name)?;
        filters.add_exact("tool_name", "tool_name", &args.tool_name)?;

        if args.tool_error_only {
            filters.conditions.push("e.tool_error != 0".to_string());
            filters
                .metadata
                .insert("tool_error_only".to_string(), Value::Bool(true));
            filters.has_filter = true;
        }

        Ok(filters)
    }

    fn has_any_filter(&self) -> bool {
        self.has_filter
    }

    fn insert_metadata_string(&mut self, key: &str, value: String) {
        self.metadata.insert(key.to_string(), Value::String(value));
        self.has_filter = true;
    }

    fn insert_metadata_array(&mut self, key: &str, values: &[String]) {
        self.metadata.insert(
            key.to_string(),
            Value::Array(values.iter().cloned().map(Value::String).collect()),
        );
        self.has_filter = true;
    }

    fn add_exact(&mut self, metadata_key: &str, column: &str, values: &[String]) -> Result<()> {
        validate_filter_values(metadata_key, values)?;
        if values.is_empty() {
            return Ok(());
        }

        let conditions = values
            .iter()
            .map(|value| format!("e.{} = {}", sql_identifier(column), sql_quote(value)))
            .collect::<Vec<_>>();
        self.conditions.push(parenthesize_or(conditions));
        self.insert_metadata_array(metadata_key, values);
        Ok(())
    }

    fn add_cwd_prefix(&mut self, values: &[String]) -> Result<()> {
        validate_filter_values("cwd_prefix", values)?;
        if values.is_empty() {
            return Ok(());
        }

        let conditions = values
            .iter()
            .map(|value| {
                let normalized = normalize_cwd_prefix(value);
                let literal = sql_quote(&normalized);
                if normalized == "/" {
                    "(e.`cwd` = '/' OR startsWith(e.`cwd`, '/'))".to_string()
                } else {
                    format!("(e.`cwd` = {literal} OR startsWith(e.`cwd`, concat({literal}, '/')))")
                }
            })
            .collect::<Vec<_>>();
        self.conditions.push(parenthesize_or(conditions));
        self.insert_metadata_array("cwd_prefix", values);
        Ok(())
    }
}

fn parse_optional_timestamp(flag: &str, raw: Option<&str>) -> Result<Option<(String, i64)>> {
    raw.map(|value| {
        let dt = DateTime::parse_from_rfc3339(value)
            .with_context(|| format!("{flag} must be an RFC3339 timestamp"))?;
        Ok((value.to_string(), dt.with_timezone(&Utc).timestamp_millis()))
    })
    .transpose()
}

fn validate_filter_values(name: &str, values: &[String]) -> Result<()> {
    for value in values {
        if value.is_empty() {
            bail!("--{} values must not be empty", name.replace('_', "-"));
        }
        if value.contains(',') {
            bail!(
                "--{} does not accept comma-separated lists in v1; repeat the flag instead",
                name.replace('_', "-")
            );
        }
    }
    Ok(())
}

fn normalize_cwd_prefix(value: &str) -> String {
    let trimmed = value.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

fn parenthesize_or(conditions: Vec<String>) -> String {
    if conditions.len() == 1 {
        conditions.into_iter().next().expect("one condition")
    } else {
        format!("({})", conditions.join(" OR "))
    }
}

fn build_events_query(
    database: &str,
    columns: &[&EventColumn],
    filters: &EventFilters,
    limit: Option<usize>,
) -> Result<String> {
    validate_identifier(database)?;
    let projections = columns
        .iter()
        .map(|column| {
            format!(
                "  {} AS {}",
                projection_expression(column),
                sql_identifier(column.name)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let where_clause = if filters.conditions.is_empty() {
        "1 = 1".to_string()
    } else {
        filters.conditions.join("\n  AND ")
    };
    let limit_clause = limit
        .map(|value| format!("\nLIMIT {}", value.saturating_add(1)))
        .unwrap_or_default();

    Ok(format!(
        "WITH latest_events AS (
  SELECT *
  FROM (
    SELECT
      e.*,
      ifNull(parseDateTime64BestEffortOrNull(record_ts), ingested_at) AS canonical_event_time,
      row_number() OVER (
        PARTITION BY event_uid
        ORDER BY event_version DESC,
                 canonical_event_time DESC,
                 source_file DESC,
                 source_generation DESC,
                 source_offset DESC,
                 source_line_no DESC,
                 event_uid DESC
      ) AS event_uid_rank
    FROM {database}.events AS e FINAL
  )
  WHERE event_uid_rank = 1
),
trace_events AS (
  SELECT
    latest_events.*,
    row_number() OVER (
      PARTITION BY session_id
      ORDER BY canonical_event_time,
               source_file,
               source_generation,
               source_offset,
               source_line_no,
               event_uid
    ) AS event_order,
    if(
      toUInt32OrZero(toString(turn_index)) > 0,
      toUInt32OrZero(toString(turn_index)),
      greatest(
        toUInt32(1),
        toUInt32(
          sum(if(actor_kind = 'user' AND event_kind = 'message', 1, 0)) OVER (
            PARTITION BY session_id
            ORDER BY canonical_event_time,
                     source_file,
                     source_generation,
                     source_offset,
                     source_line_no,
                     event_uid
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )
        )
      )
    ) AS turn_seq,
    toInt64(toUnixTimestamp64Milli(canonical_event_time)) AS event_unix_ms
  FROM latest_events
)
SELECT
{projections}
FROM trace_events AS e
WHERE {where_clause}
ORDER BY event_unix_ms ASC, session_id ASC, event_order ASC, event_uid ASC{limit_clause}
FORMAT JSONEachRow",
        database = sql_identifier(database),
    ))
}

fn projection_expression(column: &EventColumn) -> String {
    if column.name == "event_ts" {
        return "e.event_unix_ms".to_string();
    }

    if column.value_type == "boolean" {
        format!("CAST({}, 'Bool')", column.select_expression)
    } else {
        column.select_expression.to_string()
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty()
        || !identifier
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        bail!("ClickHouse database name contains unsupported characters: {identifier}");
    }
    Ok(())
}

fn sql_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

async fn stream_jsonl_rows<W: Write>(
    stream: &mut moraine_clickhouse::ClickHouseByteStream,
    writer: &mut W,
    columns: &[&EventColumn],
    limit: Option<usize>,
) -> Result<StreamRowsResult> {
    let mut framer = LineFramer::default();
    let mut result = StreamRowsResult::default();

    while let Some(chunk) = stream.next_chunk().await? {
        if framer.push_chunk(&chunk, |line| {
            process_export_line(line, writer, columns, limit, &mut result)
        })? {
            return Ok(result);
        }
    }

    if framer.finish(|line| process_export_line(line, writer, columns, limit, &mut result))? {
        return Ok(result);
    }

    if let Err(err) = writer.flush() {
        if err.kind() == ErrorKind::BrokenPipe {
            result.broken_pipe = true;
            return Ok(result);
        }
        return Err(err).context("failed to flush export stdout");
    }

    Ok(result)
}

#[cfg(test)]
fn stream_jsonl_chunks<W: Write>(
    chunks: &[&[u8]],
    writer: &mut W,
    columns: &[&EventColumn],
    limit: Option<usize>,
) -> Result<StreamRowsResult> {
    let mut framer = LineFramer::default();
    let mut result = StreamRowsResult::default();

    for chunk in chunks {
        if framer.push_chunk(chunk, |line| {
            process_export_line(line, writer, columns, limit, &mut result)
        })? {
            return Ok(result);
        }
    }

    if framer.finish(|line| process_export_line(line, writer, columns, limit, &mut result))? {
        return Ok(result);
    }
    writer.flush().context("failed to flush export writer")?;

    Ok(result)
}

fn process_export_line<W: Write>(
    line: &[u8],
    writer: &mut W,
    columns: &[&EventColumn],
    limit: Option<usize>,
    result: &mut StreamRowsResult,
) -> Result<bool> {
    if line.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(false);
    }

    if limit.is_some_and(|limit| result.row_count >= limit) {
        result.truncated = true;
        return Ok(true);
    }

    let public_row = public_row_json(line, columns)?;
    if let Err(err) = writer.write_all(&public_row) {
        if err.kind() == ErrorKind::BrokenPipe {
            result.broken_pipe = true;
            return Ok(true);
        }
        return Err(err).context("failed to write export row to stdout");
    }
    if let Err(err) = writer.write_all(b"\n") {
        if err.kind() == ErrorKind::BrokenPipe {
            result.broken_pipe = true;
            return Ok(true);
        }
        return Err(err).context("failed to write export row delimiter to stdout");
    }

    result.row_count += 1;
    Ok(false)
}

fn public_row_json(line: &[u8], columns: &[&EventColumn]) -> Result<Vec<u8>> {
    let mut raw = serde_json::from_slice::<Map<String, Value>>(line).with_context(|| {
        format!(
            "failed to parse ClickHouse JSONEachRow line: {}",
            lossy(line)
        )
    })?;
    let mut public = Map::new();

    for column in columns {
        let mut value = raw.remove(column.name).unwrap_or(Value::Null);
        if column.name == "event_ts" {
            let ms = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .ok_or_else(|| anyhow!("ClickHouse row returned non-integer event_ts sentinel"))?;
            value = Value::String(format_event_ts(ms)?);
        } else if column.value_type == "boolean" {
            value = json_value_as_bool(value);
        }
        public.insert(column.name.to_string(), value);
    }

    serde_json::to_vec(&Value::Object(public)).context("failed to serialize export JSONL row")
}

fn json_value_as_bool(value: Value) -> Value {
    match value {
        Value::Bool(_) => value,
        Value::Number(number) => Value::Bool(number.as_u64().unwrap_or(0) != 0),
        other => other,
    }
}

fn format_event_ts(unix_ms: i64) -> Result<String> {
    let dt = Utc
        .timestamp_millis_opt(unix_ms)
        .single()
        .ok_or_else(|| anyhow!("event timestamp is out of range: {unix_ms}"))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[derive(Default)]
struct LineFramer {
    pending: Vec<u8>,
}

impl LineFramer {
    fn push_chunk<F>(&mut self, chunk: &[u8], mut on_line: F) -> Result<bool>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let mut start = 0;

        if !self.pending.is_empty() {
            let Some(pos) = chunk.iter().position(|byte| *byte == b'\n') else {
                self.pending.extend_from_slice(chunk);
                return Ok(false);
            };

            self.pending.extend_from_slice(&chunk[..pos]);
            if self.emit_pending(&mut on_line)? {
                return Ok(true);
            }
            start = pos + 1;
        }

        while let Some(offset) = chunk[start..].iter().position(|byte| *byte == b'\n') {
            let end = start + offset;
            let line = trim_cr(&chunk[start..end]);
            if on_line(line)? {
                return Ok(true);
            }
            start = end + 1;
        }

        if start < chunk.len() {
            self.pending.extend_from_slice(&chunk[start..]);
        }
        Ok(false)
    }

    fn finish<F>(&mut self, mut on_line: F) -> Result<bool>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        if self.pending.is_empty() {
            return Ok(false);
        }
        self.emit_pending(&mut on_line)
    }

    fn emit_pending<F>(&mut self, on_line: &mut F) -> Result<bool>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        let stop = {
            let line = trim_cr(&self.pending);
            on_line(line)?
        };
        self.pending.clear();
        Ok(stop)
    }
}

fn trim_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn write_completion_metadata(metadata: &CompletionMetadata<'_>) -> Result<()> {
    let mut stderr = std::io::stderr().lock();
    write_completion_metadata_line(&mut stderr, metadata)
        .context("failed to write export metadata to stderr")
}

fn completion_metadata_json(metadata: &CompletionMetadata<'_>) -> Result<String> {
    serde_json::to_string(metadata).context("failed to serialize export metadata")
}

fn write_completion_metadata_line<W: Write>(
    writer: &mut W,
    metadata: &CompletionMetadata<'_>,
) -> Result<()> {
    let json = completion_metadata_json(metadata)?;
    writeln!(writer, "{json}").context("failed to write export metadata")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ExportRowFormat;
    use serde_json::json;

    fn base_args() -> ExportEventsArgs {
        ExportEventsArgs {
            format: ExportRowFormat::Jsonl,
            columns: None,
            include_sensitive: false,
            limit: None,
            all: false,
            since: None,
            until: None,
            session_id: Vec::new(),
            harness: Vec::new(),
            source_name: Vec::new(),
            project_id: Vec::new(),
            cwd_prefix: Vec::new(),
            worktree_root: Vec::new(),
            repo_rel_path: Vec::new(),
            event_kind: Vec::new(),
            payload_type: Vec::new(),
            actor_kind: Vec::new(),
            model_name: Vec::new(),
            tool_name: Vec::new(),
            tool_error_only: false,
        }
    }

    #[test]
    fn select_columns_defaults_and_all_omit_sensitive() {
        let defaults = select_columns(None, false).expect("default columns");
        assert_eq!(
            defaults.first().map(|column| column.name),
            Some("session_id")
        );
        assert!(defaults.iter().all(|column| !column.sensitive));
        assert!(defaults.iter().all(|column| column.name != "text_preview"));

        let all = select_columns(Some("all"), true).expect("all columns");
        assert!(all.iter().any(|column| column.name == "event_unix_ms"));
        assert!(all.iter().all(|column| !column.sensitive));
        assert!(all.iter().all(|column| column.name != "text_preview"));
    }

    #[test]
    fn select_columns_rejects_sensitive_without_opt_in() {
        let err = select_columns(Some("session_id,payload_json"), false)
            .expect_err("sensitive column should require opt-in");
        assert!(err.to_string().contains("--include-sensitive"));
    }

    #[test]
    fn select_columns_rejects_empty_unknown_and_duplicate_names() {
        assert!(select_columns(Some("session_id,"), false)
            .expect_err("empty")
            .to_string()
            .contains("empty"));
        assert!(select_columns(Some("session_id,nope"), false)
            .expect_err("unknown")
            .to_string()
            .contains("unsupported export column"));
        assert!(select_columns(Some("session_id,session_id"), false)
            .expect_err("duplicate")
            .to_string()
            .contains("duplicate"));
    }

    #[test]
    fn filters_reject_empty_and_comma_lists() {
        let mut args = base_args();
        args.session_id.push(String::new());
        assert!(EventFilters::from_args(&args)
            .expect_err("empty filter")
            .to_string()
            .contains("must not be empty"));

        let mut args = base_args();
        args.harness.push("codex,hermes".to_string());
        assert!(EventFilters::from_args(&args)
            .expect_err("comma list")
            .to_string()
            .contains("repeat the flag"));
    }

    #[test]
    fn filters_parse_time_bounds_and_validate_order() {
        let mut args = base_args();
        args.since = Some("2026-06-01T00:00:00Z".to_string());
        args.until = Some("2026-06-15T00:00:00Z".to_string());
        let filters = EventFilters::from_args(&args).expect("filters");
        assert!(filters
            .conditions
            .iter()
            .any(|condition| condition == "e.event_unix_ms >= 1780272000000"));
        assert!(filters
            .conditions
            .iter()
            .any(|condition| condition == "e.event_unix_ms < 1781481600000"));

        args.until = Some("2026-05-01T00:00:00Z".to_string());
        assert!(EventFilters::from_args(&args)
            .expect_err("bad order")
            .to_string()
            .contains("earlier"));
    }

    #[test]
    fn filters_normalize_cwd_prefix_trailing_slash_and_root() {
        let mut args = base_args();
        args.cwd_prefix = vec!["/repo/".to_string(), "/".to_string()];
        let filters = EventFilters::from_args(&args).expect("filters");
        let sql = build_events_query(
            "moraine",
            &select_columns(Some("event_uid"), false).expect("columns"),
            &filters,
            None,
        )
        .expect("sql");

        assert!(
            sql.contains("(e.`cwd` = '/repo' OR startsWith(e.`cwd`, concat('/repo', '/')))"),
            "{sql}"
        );
        assert!(
            sql.contains("(e.`cwd` = '/' OR startsWith(e.`cwd`, '/'))"),
            "{sql}"
        );
    }

    #[test]
    fn query_builder_applies_filters_after_dedupe_and_uses_limit_sentinel() {
        let mut args = base_args();
        args.harness = vec!["codex".to_string(), "hermes".to_string()];
        args.cwd_prefix = vec!["/repo".to_string()];
        let filters = EventFilters::from_args(&args).expect("filters");
        let columns = select_columns(Some("session_id,event_uid,event_ts,tool_error"), false)
            .expect("columns");
        let sql = build_events_query("moraine", &columns, &filters, Some(100)).expect("sql");

        assert!(sql.contains("row_number() OVER (\n        PARTITION BY event_uid"));
        assert!(sql.contains("FROM `moraine`.events AS e FINAL"));
        assert!(sql.contains("FROM trace_events AS e\nWHERE"));
        assert!(sql.contains("(e.`harness` = 'codex' OR e.`harness` = 'hermes')"));
        assert!(sql.contains("(e.`cwd` = '/repo' OR startsWith(e.`cwd`, concat('/repo', '/')))"));
        assert!(sql.contains("LIMIT 101"));
        assert!(sql.contains("CAST(e.tool_error != 0, 'Bool') AS `tool_error`"));
        assert!(sql.ends_with("FORMAT JSONEachRow"));
    }

    #[test]
    fn sql_quote_escapes_quotes_backslashes_and_semicolons_as_data() {
        let mut args = base_args();
        args.tool_name = vec!["tool\\name'; DROP TABLE moraine.events; --".to_string()];
        let filters = EventFilters::from_args(&args).expect("filters");
        let sql = build_events_query(
            "moraine",
            &select_columns(Some("event_uid"), false).expect("columns"),
            &filters,
            None,
        )
        .expect("sql");

        assert!(sql.contains("tool\\\\name''; DROP TABLE moraine.events; --"));
        assert!(!sql.contains("tool\\name'; DROP"));
    }

    #[test]
    fn prepare_export_requires_format_filter_and_positive_limit() {
        let cfg = AppConfig::default();
        let mut args = base_args();
        args.all = true;
        args.limit = Some(0);
        assert!(prepare_export(&cfg, args)
            .expect_err("zero limit")
            .to_string()
            .contains("positive"));

        let args = base_args();
        assert!(prepare_export(&cfg, args)
            .expect_err("missing filter")
            .to_string()
            .contains("at least one filter"));
    }

    #[test]
    fn query_params_use_readonly_and_internal_execution_limit() {
        let params = build_query_params("qid");
        assert_eq!(
            params,
            vec![
                ("query_id".to_string(), "qid".to_string()),
                ("readonly".to_string(), "1".to_string()),
                (
                    "max_execution_time".to_string(),
                    DEFAULT_MAX_EXECUTION_SECONDS.to_string()
                ),
            ]
        );
    }

    #[test]
    fn request_timeout_outlives_internal_execution_setting() {
        assert_eq!(request_timeout(30.0), Duration::from_secs(630));
        assert_eq!(request_timeout(900.0), Duration::from_secs(900));
    }

    #[test]
    fn format_event_ts_uses_utc_millisecond_precision() {
        assert_eq!(
            format_event_ts(1780317296789).expect("timestamp"),
            "2026-06-01T12:34:56.789Z"
        );
    }

    #[test]
    fn stream_chunks_frames_rows_formats_timestamps_and_handles_truncation() {
        let columns =
            select_columns(Some("event_uid,event_ts,tool_error"), false).expect("columns");
        let chunks = [
            br#"{"event_uid":"a","event_ts":1780317296789,"tool_error":0}"#.as_slice(),
            b"\n",
            br#"{"event_uid":"b","event_ts":1780317296790,"tool_error":1}"#.as_slice(),
            b"\n",
            br#"{"event_uid":"sentinel","event_ts":1780317296791,"tool_error":0}"#.as_slice(),
            b"\n",
        ];
        let mut out = Vec::new();
        let result =
            stream_jsonl_chunks(&chunks, &mut out, &columns, Some(2)).expect("stream chunks");

        assert_eq!(
            result,
            StreamRowsResult {
                row_count: 2,
                truncated: true,
                broken_pipe: false,
            }
        );
        let rows = String::from_utf8(out).expect("utf8");
        assert!(rows.contains("\"event_ts\":\"2026-06-01T12:34:56.789Z\""));
        assert!(rows.contains("\"tool_error\":false"));
        assert!(rows.contains("\"tool_error\":true"));
        assert!(!rows.contains("sentinel"));
    }

    #[test]
    fn stream_chunks_frames_multiple_rows_from_one_chunk() {
        let columns = select_columns(Some("event_uid,event_ts"), false).expect("columns");
        let chunks = [br#"{"event_uid":"a","event_ts":1780317296789}
{"event_uid":"b","event_ts":1780317296790}
{"event_uid":"c","event_ts":1780317296791}
"#
        .as_slice()];
        let mut out = Vec::new();
        let result = stream_jsonl_chunks(&chunks, &mut out, &columns, None).expect("stream chunks");

        assert_eq!(
            result,
            StreamRowsResult {
                row_count: 3,
                truncated: false,
                broken_pipe: false,
            }
        );
        let rows = String::from_utf8(out).expect("utf8");
        assert!(rows.contains("\"event_uid\":\"a\""));
        assert!(rows.contains("\"event_uid\":\"b\""));
        assert!(rows.contains("\"event_uid\":\"c\""));
    }

    struct BrokenPipeWriter;

    impl Write for BrokenPipeWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                ErrorKind::BrokenPipe,
                "test pipe closed",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn stream_chunks_treats_broken_pipe_as_success() {
        let columns = select_columns(Some("event_uid,event_ts"), false).expect("columns");
        let chunks = [br#"{"event_uid":"a","event_ts":1780317296789}"#.as_slice()];
        let mut writer = BrokenPipeWriter;
        let result =
            stream_jsonl_chunks(&chunks, &mut writer, &columns, None).expect("stream chunks");

        assert_eq!(
            result,
            StreamRowsResult {
                row_count: 0,
                truncated: false,
                broken_pipe: true,
            }
        );
    }

    fn sample_metadata() -> CompletionMetadata<'static> {
        let mut filters = BTreeMap::new();
        filters.insert("harness".to_string(), json!(["codex"]));
        CompletionMetadata {
            schema_version: EXPORT_METADATA_SCHEMA_VERSION,
            data_schema_version: EVENTS_SCHEMA_VERSION,
            export_kind: EXPORT_KIND_EVENTS,
            backend: DEFAULT_BACKEND,
            query_id: "qid-test",
            columns: vec!["session_id", "event_uid"],
            filters,
            limit: Some(100),
            row_count: 42,
            truncated: false,
            elapsed_ms: 123,
            sensitive_columns_requested: Vec::new(),
        }
    }

    #[test]
    fn metadata_json_line_matches_stderr_payload() {
        let metadata = sample_metadata();
        let mut out = Vec::new();

        write_completion_metadata_line(&mut out, &metadata).expect("metadata line");

        let value: Value =
            serde_json::from_slice(&out).expect("metadata line should be valid JSON");
        assert_eq!(
            value["schema_version"],
            Value::String(EXPORT_METADATA_SCHEMA_VERSION.to_string())
        );
        assert_eq!(
            value["filters"]["harness"][0],
            Value::String("codex".to_string())
        );
        assert!(String::from_utf8(out).expect("utf8").ends_with('\n'));
    }
}
