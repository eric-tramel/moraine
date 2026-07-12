use super::*;

#[derive(Debug, Deserialize)]
struct SessionAnalyticsEventRow {
    session_id: String,
    event_order: u64,
    turn_seq: u32,
    event_unix_ms: i64,
    event_class: String,
    actor_role: String,
    payload_type: String,
    call_id: String,
    tool_name: String,
    tool_error: u8,
    latency_ms: u32,
    model: String,
    endpoint_kind: String,
    input_tokens: u32,
    output_tokens: u32,
    cache_read_tokens: u32,
    cache_write_tokens: u32,
    #[serde(default)]
    token_usage_buckets: BTreeMap<String, u64>,
    #[serde(default)]
    token_usage_native_units: BTreeMap<String, f64>,
    text_preview: String,
    text_content: String,
    tool_args_json: String,
    harness: String,
    source_name: String,
    trace_id: String,
}

#[derive(Debug, Deserialize)]
struct AnalyticsAnchorRow {
    scan_from_unix: u64,
    scan_to_unix: u64,
    display_to_unix: u64,
}

#[derive(Debug, Deserialize)]
struct AnalyticsTokenRow {
    bucket_unix: u64,
    model: String,
    endpoint_kind: String,
    bucket: String,
    tokens: u64,
}

#[derive(Debug, Deserialize)]
struct AnalyticsTurnRow {
    bucket_unix: u64,
    model: String,
    turns: u64,
}

#[derive(Debug, Deserialize)]
struct AnalyticsConcurrencyRow {
    bucket_unix: u64,
    concurrent_sessions: u64,
}

#[derive(Debug, Deserialize)]
struct WebSearchRow {
    event_time: String,
    harness: String,
    source_name: String,
    session_id: String,
    model: String,
    action: String,
    search_query: String,
    result_url: String,
    source_ref: String,
}

impl ClickHouseConversationRepository {
    pub(super) async fn list_session_analytics_impl(
        &self,
        query: SessionAnalyticsQuery,
    ) -> RepoResult<Vec<SessionAnalytics>> {
        let session_summary = self.table_ref("v_session_summary");
        let turn_summary = self.table_ref("v_turn_summary");
        let trace = self.table_ref("v_conversation_trace");
        let canonical_events = canonical_events_source(&self.table_ref("events"));
        let mode_subquery = self.mode_subquery();
        let lookback_clause = query
            .lookback
            .window_seconds()
            .map(|seconds| {
                format!("\n  AND s.last_event_time >= now() - INTERVAL {seconds} SECOND")
            })
            .unwrap_or_default();
        let limit = query.normalized_limit();

        let summary_query = format!(
            "SELECT
  s.session_id AS session_id,
  toString(s.first_event_time) AS first_event_time,
  toInt64(toUnixTimestamp64Milli(s.first_event_time)) AS first_event_unix_ms,
  toString(s.last_event_time) AS last_event_time,
  toInt64(toUnixTimestamp64Milli(s.last_event_time)) AS last_event_unix_ms,
  toUInt32(s.total_turns) AS total_turns,
  toUInt64(s.total_events) AS total_events,
  toUInt64(s.user_messages) AS user_messages,
  toUInt64(s.assistant_messages) AS assistant_messages,
  toUInt64(s.tool_calls) AS tool_calls,
  toUInt64(s.tool_results) AS tool_results,
  ifNull(m.mode, 'chat') AS mode,
  ifNull(meta.session_slug, '') AS session_slug,
  ifNull(meta.session_summary, '') AS session_summary
FROM {session_summary} AS s
LEFT JOIN ({mode_subquery}) AS m ON m.session_id = s.session_id
LEFT JOIN (
  SELECT
    session_id,
    ifNull(argMax(nullIf(JSONExtractString(payload_json, 'slug'), ''), tuple(event_ts, event_uid)), '') AS session_slug,
    ifNull(
      argMax(
        coalesce(
          nullIf(JSONExtractString(payload_json, 'summary'), ''),
          nullIf(JSONExtractString(payload_json, 'title'), ''),
          nullIf(JSONExtractString(payload_json, 'name'), '')
        ),
        tuple(event_ts, event_uid)
      ),
      ''
    ) AS session_summary
  FROM {canonical_events} AS meta_events
  WHERE meta_events.event_kind = 'session_meta'
  GROUP BY session_id
) AS meta ON meta.session_id = s.session_id
WHERE notEmpty(trimBoth(s.session_id)){lookback_clause}
ORDER BY s.last_event_time DESC, s.session_id DESC
LIMIT {limit}
FORMAT JSONEachRow"
        );
        let summary_rows: Vec<ConversationSummaryRow> =
            self.map_backend(self.query_rows(&summary_query, None).await)?;
        if summary_rows.is_empty() {
            return Ok(Vec::new());
        }

        let session_ids = summary_rows
            .iter()
            .map(|row| row.session_id.clone())
            .collect::<Vec<_>>();
        let session_ids_sql = sql_array_strings(&session_ids);

        let turns_query = format!(
            "SELECT
  session_id,
  toUInt32(turn_seq) AS turn_seq,
  ifNull(turn_id, '') AS turn_id,
  toString(started_at) AS started_at,
  toInt64(toUnixTimestamp64Milli(parseDateTime64BestEffort(toString(started_at), 3, 'UTC'))) AS started_at_unix_ms,
  toString(ended_at) AS ended_at,
  toInt64(toUnixTimestamp64Milli(parseDateTime64BestEffort(toString(ended_at), 3, 'UTC'))) AS ended_at_unix_ms,
  toUInt64(total_events) AS total_events,
  toUInt64(user_messages) AS user_messages,
  toUInt64(assistant_messages) AS assistant_messages,
  toUInt64(tool_calls) AS tool_calls,
  toUInt64(tool_results) AS tool_results,
  toUInt64(reasoning_items) AS reasoning_items
FROM {turn_summary}
WHERE session_id IN {session_ids_sql}
ORDER BY session_id ASC, turn_seq ASC
FORMAT JSONEachRow"
        );
        let turn_rows: Vec<TurnSummaryRow> =
            self.map_backend(self.query_rows(&turns_query, None).await)?;

        let events_query = format!(
            "SELECT
  t.session_id AS session_id,
  toUInt64(t.event_order) AS event_order,
  toUInt32(t.turn_seq) AS turn_seq,
  toInt64(toUnixTimestamp64Milli(t.event_time)) AS event_unix_ms,
  t.event_class AS event_class,
  t.actor_role AS actor_role,
  t.payload_type AS payload_type,
  ifNull(t.call_id, '') AS call_id,
  ifNull(t.name, '') AS tool_name,
  toUInt8(ifNull(e.tool_error, 0)) AS tool_error,
  toUInt32(ifNull(e.latency_ms, 0)) AS latency_ms,
  ifNull(e.model, '') AS model,
  ifNull(t.endpoint_kind, '') AS endpoint_kind,
  toUInt32(ifNull(e.input_tokens, 0)) AS input_tokens,
  toUInt32(ifNull(e.output_tokens, 0)) AS output_tokens,
  toUInt32(ifNull(e.cache_read_tokens, 0)) AS cache_read_tokens,
  toUInt32(ifNull(e.cache_write_tokens, 0)) AS cache_write_tokens,
  t.token_usage_buckets AS token_usage_buckets,
  t.token_usage_native_units AS token_usage_native_units,
  substringUTF8(ifNull(e.text_preview, ''), 1, 2000) AS text_preview,
  substringUTF8(ifNull(t.text_content, ''), 1, 2000) AS text_content,
  if(t.event_class = 'tool_call', substringUTF8(JSONExtractRaw(t.payload_json, 'input'), 1, 2000), '') AS tool_args_json,
  ifNull(e.harness, '') AS harness,
  ifNull(e.source_name, '') AS source_name,
  ifNull(e.trace_id, '') AS trace_id
FROM {trace} AS t
LEFT JOIN {canonical_events} AS e ON e.event_uid = t.event_uid
WHERE t.session_id IN {session_ids_sql}
ORDER BY t.session_id ASC, t.event_order ASC
FORMAT JSONEachRow"
        );
        let event_rows: Vec<SessionAnalyticsEventRow> =
            self.map_backend(self.query_rows(&events_query, None).await)?;

        Ok(assemble_sessions(summary_rows, turn_rows, event_rows))
    }

    pub(super) async fn analytics_series_impl(
        &self,
        range: AnalyticsRange,
    ) -> RepoResult<AnalyticsSnapshot> {
        let slot = &self.analytics_cache[analytics_range_index(range)];
        let mut entry = slot.lock().await;
        let now = Instant::now();
        if let Some(cached) = entry.as_ref().filter(|cached| cached.is_fresh(now)) {
            return Ok(cached.snapshot.clone());
        }

        let snapshot = self.load_analytics_snapshot(range).await?;
        *entry = Some(AnalyticsCacheEntry {
            snapshot: snapshot.clone(),
            fetched_at: Instant::now(),
        });
        Ok(snapshot)
    }

    async fn load_analytics_snapshot(
        &self,
        range: AnalyticsRange,
    ) -> RepoResult<AnalyticsSnapshot> {
        let canonical_events = canonical_events_source(&self.table_ref("events"));
        let window_seconds = range.window_seconds();
        let bucket_seconds = range.bucket_seconds();
        let anchor_query = format!(
            "WITH toInt64(toUnixTimestamp(now())) AS database_now_unix
SELECT
  toUInt64(greatest(database_now_unix - toInt64({window_seconds}), toInt64(0))) AS scan_from_unix,
  toUInt64(database_now_unix) AS scan_to_unix,
  toUInt64(if(count() = 0, database_now_unix, max(intDiv(toUnixTimestamp64Milli(e.event_ts), 1000)))) AS display_to_unix
FROM {canonical_events} AS e
WHERE intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= greatest(database_now_unix - toInt64({window_seconds}), toInt64(0))
  AND intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) <= database_now_unix
  AND notEmpty(trimBoth(e.model))
  AND lowerUTF8(trimBoth(e.model)) != '<synthetic>'
FORMAT JSONEachRow"
        );
        let anchors: Vec<AnalyticsAnchorRow> =
            self.map_backend(self.query_rows(&anchor_query, None).await)?;
        let anchor = anchors
            .into_iter()
            .next()
            .ok_or_else(|| RepoError::backend("analytics anchor query returned no row"))?;
        let event_bounds = format!(
            "intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) >= {} AND intDiv(toUnixTimestamp64Milli(e.event_ts), 1000) <= {}",
            anchor.scan_from_unix, anchor.scan_to_unix
        );
        let model_expr = "if(lowerUTF8(trimBoth(e.model)) = 'codex', 'gpt-5.3-codex-xhigh', lowerUTF8(trimBoth(e.model)))";
        let eligible_model =
            "notEmpty(trimBoth(e.model)) AND lowerUTF8(trimBoth(e.model)) != '<synthetic>'";

        let token_query = format!(
            "SELECT
  bucket_unix,
  model,
  endpoint_kind,
  bucket,
  toUInt64(sum(tokens)) AS tokens
FROM (
  SELECT
    bucket_unix,
    model,
    endpoint_kind,
    bucket,
    toUInt64(max(tokens_per_event)) AS tokens
  FROM (
    SELECT
      toUInt64(toUnixTimestamp(toStartOfInterval(e.event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix,
      {model_expr} AS model,
      e.endpoint_kind AS endpoint_kind,
      e.session_id AS session_id,
      e.request_id AS request_id,
      bucket,
      toUInt64(tokens_per_event) AS tokens_per_event
    FROM {canonical_events} AS e
    ARRAY JOIN mapKeys(e.token_usage_buckets) AS bucket, mapValues(e.token_usage_buckets) AS tokens_per_event
    WHERE {event_bounds}
      AND {eligible_model}
      AND tokens_per_event > 0
      AND e.harness = 'claude-code'
      AND notEmpty(trimBoth(e.request_id))
  )
  GROUP BY bucket_unix, model, endpoint_kind, session_id, request_id, bucket
  UNION ALL
  SELECT
    toUInt64(toUnixTimestamp(toStartOfInterval(e.event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix,
    {model_expr} AS model,
    e.endpoint_kind AS endpoint_kind,
    bucket,
    toUInt64(tokens_per_event) AS tokens
  FROM {canonical_events} AS e
  ARRAY JOIN mapKeys(e.token_usage_buckets) AS bucket, mapValues(e.token_usage_buckets) AS tokens_per_event
  WHERE {event_bounds}
    AND {eligible_model}
    AND tokens_per_event > 0
    AND NOT (e.harness = 'claude-code' AND notEmpty(trimBoth(e.request_id)))
)
GROUP BY bucket_unix, model, endpoint_kind, bucket
ORDER BY bucket_unix ASC, model ASC, endpoint_kind ASC, bucket ASC
FORMAT JSONEachRow"
        );
        let token_rows: Vec<AnalyticsTokenRow> =
            self.map_backend(self.query_rows(&token_query, None).await)?;

        let turns_query = format!(
            "SELECT
  toUInt64(toUnixTimestamp(toStartOfInterval(e.event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix,
  {model_expr} AS model,
  toUInt64(uniqExact(tuple(e.session_id, e.request_id))) AS turns
FROM {canonical_events} AS e
WHERE {event_bounds}
  AND {eligible_model}
  AND notEmpty(trimBoth(e.request_id))
GROUP BY bucket_unix, model
ORDER BY bucket_unix ASC, model ASC
FORMAT JSONEachRow"
        );
        let turn_rows: Vec<AnalyticsTurnRow> =
            self.map_backend(self.query_rows(&turns_query, None).await)?;

        let concurrency_query = format!(
            "SELECT
  bucket_unix,
  toUInt64(uniqExact(session_stream_key)) AS concurrent_sessions
FROM (
  SELECT
    toUInt64(toUnixTimestamp(toStartOfInterval(e.event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix,
    if(
      e.harness = 'claude-code' AND notEmpty(trimBoth(e.agent_run_id)),
      concat(e.session_id, '::', e.agent_run_id),
      e.session_id
    ) AS session_stream_key
  FROM {canonical_events} AS e
  WHERE {event_bounds}
    AND notEmpty(trimBoth(e.session_id))
    AND arraySum(mapValues(e.token_usage_buckets)) > 0
)
GROUP BY bucket_unix
ORDER BY bucket_unix ASC
FORMAT JSONEachRow"
        );
        let concurrency_rows: Vec<AnalyticsConcurrencyRow> =
            self.map_backend(self.query_rows(&concurrency_query, None).await)?;

        Ok(AnalyticsSnapshot {
            window: AnalyticsWindow {
                range,
                window_seconds,
                bucket_seconds,
                from_unix: anchor
                    .display_to_unix
                    .saturating_sub(u64::from(window_seconds)),
                to_unix: anchor.display_to_unix,
            },
            tokens: token_rows
                .into_iter()
                .map(|row| AnalyticsTokenPoint {
                    bucket_unix: row.bucket_unix,
                    model: row.model,
                    endpoint_kind: row.endpoint_kind,
                    bucket: row.bucket,
                    tokens: row.tokens,
                })
                .collect(),
            turns: turn_rows
                .into_iter()
                .map(|row| AnalyticsTurnPoint {
                    bucket_unix: row.bucket_unix,
                    model: row.model,
                    turns: row.turns,
                })
                .collect(),
            concurrent_sessions: concurrency_rows
                .into_iter()
                .map(|row| AnalyticsConcurrencyPoint {
                    bucket_unix: row.bucket_unix,
                    concurrent_sessions: row.concurrent_sessions,
                })
                .collect(),
        })
    }

    pub(super) async fn list_web_searches_impl(
        &self,
        limit: u16,
    ) -> RepoResult<Vec<WebSearchEvent>> {
        let canonical_events = canonical_events_source(&self.table_ref("events"));
        let limit = limit.clamp(1, 1000);
        let query = format!(
            "SELECT
  toString(e.event_ts) AS event_time,
  e.harness AS harness,
  e.source_name AS source_name,
  e.session_id AS session_id,
  lowerUTF8(trimBoth(e.model)) AS model,
  if(
    e.payload_type = 'web_search_call',
    e.op_kind,
    if(e.tool_name = 'WebFetch', 'open_page', if(e.tool_name = 'WebSearch', 'search', e.payload_type))
  ) AS action,
  if(
    length(JSONExtractString(e.payload_json, 'action', 'query')) > 0,
    JSONExtractString(e.payload_json, 'action', 'query'),
    if(
      length(JSONExtractString(e.payload_json, 'input', 'query')) > 0,
      JSONExtractString(e.payload_json, 'input', 'query'),
      if(
        length(JSONExtractString(e.payload_json, 'data', 'query')) > 0,
        JSONExtractString(e.payload_json, 'data', 'query'),
        e.text_content
      )
    )
  ) AS search_query,
  if(
    length(JSONExtractString(e.payload_json, 'action', 'url')) > 0,
    JSONExtractString(e.payload_json, 'action', 'url'),
    JSONExtractString(e.payload_json, 'input', 'url')
  ) AS result_url,
  e.source_ref AS source_ref
FROM {canonical_events} AS e
WHERE e.payload_type = 'web_search_call'
   OR (e.payload_type = 'tool_use' AND e.tool_name IN ('WebSearch', 'WebFetch'))
   OR e.payload_type = 'search_results_received'
ORDER BY e.event_ts DESC, e.event_uid DESC
LIMIT {limit}
FORMAT JSONEachRow"
        );
        let rows: Vec<WebSearchRow> = self.map_backend(self.query_rows(&query, None).await)?;
        Ok(rows
            .into_iter()
            .map(|row| WebSearchEvent {
                event_time: row.event_time,
                harness: row.harness,
                source_name: row.source_name,
                session_id: row.session_id,
                model: row.model,
                action: row.action,
                search_query: row.search_query,
                result_url: row.result_url,
                source_ref: row.source_ref,
            })
            .collect())
    }
}

fn assemble_sessions(
    summary_rows: Vec<ConversationSummaryRow>,
    turn_rows: Vec<TurnSummaryRow>,
    event_rows: Vec<SessionAnalyticsEventRow>,
) -> Vec<SessionAnalytics> {
    let mut turns_by_session = HashMap::<String, Vec<TurnSummaryRow>>::new();
    for turn in turn_rows {
        turns_by_session
            .entry(turn.session_id.clone())
            .or_default()
            .push(turn);
    }

    let mut events_by_session = HashMap::<String, Vec<SessionAnalyticsEventRow>>::new();
    for event in event_rows {
        events_by_session
            .entry(event.session_id.clone())
            .or_default()
            .push(event);
    }

    summary_rows
        .into_iter()
        .map(|row| {
            let session_id = row.session_id.clone();
            let mut events = events_by_session.remove(&session_id).unwrap_or_default();
            events.sort_by_key(|event| event.event_order);
            let harness = first_nonempty(&events, |event| &event.harness);
            let source_name = first_nonempty(&events, |event| &event.source_name);
            let trace_id = first_nonempty(&events, |event| &event.trace_id);
            let first_user_text = events
                .iter()
                .find(|event| event.event_class == "message" && event.actor_role == "user")
                .map(preferred_event_text)
                .map(|text| truncate_chars(&text, 400))
                .unwrap_or_default();
            let mut models = events
                .iter()
                .filter_map(|event| normalized_session_model(&event.model))
                .collect::<Vec<_>>();
            models.sort_unstable();
            models.dedup();

            let mut session_turns = turns_by_session.remove(&session_id).unwrap_or_default();
            session_turns.sort_by_key(|turn| turn.turn_seq);
            let mut events_by_turn = HashMap::<u32, Vec<SessionAnalyticsEventRow>>::new();
            for event in events {
                events_by_turn
                    .entry(event.turn_seq)
                    .or_default()
                    .push(event);
            }
            let turns = session_turns
                .into_iter()
                .map(|turn| {
                    let events = events_by_turn.remove(&turn.turn_seq).unwrap_or_default();
                    assemble_turn(turn, events)
                })
                .collect();

            SessionAnalytics {
                summary: ClickHouseConversationRepository::map_conversation_row(row),
                harness,
                source_name,
                models,
                trace_id,
                first_user_text,
                turns,
            }
        })
        .collect()
}

fn assemble_turn(row: TurnSummaryRow, mut events: Vec<SessionAnalyticsEventRow>) -> SessionTurn {
    let summary = ClickHouseConversationRepository::map_turn_row(row);
    events.sort_by_key(|event| event.event_order);
    let model = events
        .iter()
        .find_map(|event| trimmed_session_model(&event.model))
        .unwrap_or_default();
    let mut turn_buckets = BTreeMap::new();
    for event in &events {
        merge_token_buckets(&mut turn_buckets, &event_token_buckets(event));
    }

    let mut steps = Vec::<SessionStep>::new();
    let mut open_tool_calls = HashMap::<String, Vec<usize>>::new();
    for event in events {
        let is_user = event.event_class == "message" && event.actor_role == "user";
        let is_assistant = event.actor_role == "assistant"
            && (event.event_class == "message" || event.payload_type == "text");
        let is_thinking = event.event_class == "reasoning" || event.payload_type == "thinking";
        let is_tool_call = event.event_class == "tool_call" || event.payload_type == "tool_use";
        let is_tool_result = event.event_class == "tool_result"
            || (event.actor_role == "tool" && event.payload_type == "tool_result");

        if is_user {
            steps.push(SessionStep::User {
                event_unix_ms: event.event_unix_ms,
                text: preferred_event_text(&event),
            });
        } else if is_assistant {
            steps.push(SessionStep::Assistant {
                event_unix_ms: event.event_unix_ms,
                text: preferred_event_text(&event),
                endpoint_kind: event.endpoint_kind.clone(),
                latency_ms: (event.latency_ms > 0).then_some(event.latency_ms),
                token_usage_buckets: event_token_buckets(&event),
                token_usage_native_units: event
                    .token_usage_native_units
                    .iter()
                    .filter(|(_, value)| **value > 0.0)
                    .map(|(key, value)| (key.clone(), *value))
                    .collect(),
            });
        } else if is_thinking {
            let text = preferred_event_text(&event);
            if !text.is_empty() {
                steps.push(SessionStep::Thinking {
                    event_unix_ms: event.event_unix_ms,
                    text,
                });
            }
        } else if is_tool_call {
            let step_index = steps.len();
            if !event.call_id.is_empty() {
                open_tool_calls
                    .entry(event.call_id.clone())
                    .or_default()
                    .push(step_index);
            }
            steps.push(SessionStep::ToolCall {
                event_unix_ms: event.event_unix_ms,
                tool_name: if event.tool_name.is_empty() {
                    "tool".to_string()
                } else {
                    event.tool_name.clone()
                },
                latency_ms: (event.latency_ms > 0).then_some(event.latency_ms),
                is_error: event.tool_error != 0,
                call_id: event.call_id,
                arguments: parse_tool_arguments(&event.tool_args_json),
                result: None,
            });
        } else if is_tool_result && !event.call_id.is_empty() {
            let mut remove_key = false;
            if let Some(indices) = open_tool_calls.get_mut(&event.call_id) {
                if let Some(step_index) = indices.pop() {
                    if let Some(SessionStep::ToolCall {
                        event_unix_ms,
                        result,
                        ..
                    }) = steps.get_mut(step_index)
                    {
                        let elapsed = event.event_unix_ms.saturating_sub(*event_unix_ms);
                        *result = Some(ToolResult {
                            event_unix_ms: event.event_unix_ms,
                            text: preferred_event_text(&event),
                            latency_ms: u32::try_from(elapsed).unwrap_or(u32::MAX),
                            is_error: event.tool_error != 0,
                        });
                    }
                }
                remove_key = indices.is_empty();
            }
            if remove_key {
                open_tool_calls.remove(&event.call_id);
            }
        }
    }

    SessionTurn {
        summary,
        model,
        token_usage_buckets: turn_buckets,
        steps,
    }
}

fn event_token_buckets(event: &SessionAnalyticsEventRow) -> BTreeMap<String, u64> {
    if event
        .token_usage_buckets
        .values()
        .copied()
        .fold(0u64, u64::saturating_add)
        > 0
    {
        return event
            .token_usage_buckets
            .iter()
            .filter(|(_, value)| **value > 0)
            .map(|(key, value)| (key.clone(), *value))
            .collect();
    }

    [
        ("input_text", u64::from(event.input_tokens)),
        ("output_text", u64::from(event.output_tokens)),
        ("input_cache_read", u64::from(event.cache_read_tokens)),
        ("input_cache_write", u64::from(event.cache_write_tokens)),
    ]
    .into_iter()
    .filter(|(_, value)| *value > 0)
    .map(|(key, value)| (key.to_string(), value))
    .collect()
}

fn merge_token_buckets(target: &mut BTreeMap<String, u64>, source: &BTreeMap<String, u64>) {
    for (bucket, tokens) in source {
        let total = target.entry(bucket.clone()).or_default();
        *total = total.saturating_add(*tokens);
    }
}

fn preferred_event_text(event: &SessionAnalyticsEventRow) -> String {
    let text = if event.text_content.trim().is_empty() {
        event.text_preview.as_str()
    } else {
        event.text_content.as_str()
    };
    text.trim().to_string()
}

fn parse_tool_arguments(raw: &str) -> Value {
    if raw.trim().is_empty() {
        Value::Object(Default::default())
    } else {
        serde_json::from_str(raw).unwrap_or_else(|_| Value::Object(Default::default()))
    }
}

fn normalized_session_model(raw: &str) -> Option<String> {
    let normalized = raw.trim().to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

fn trimmed_session_model(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn first_nonempty(
    events: &[SessionAnalyticsEventRow],
    value: impl Fn(&SessionAnalyticsEventRow) -> &String,
) -> String {
    events
        .iter()
        .map(value)
        .find(|candidate| !candidate.trim().is_empty())
        .map(|candidate| candidate.trim().to_string())
        .unwrap_or_default()
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event_order: u64, event_class: &str, call_id: &str) -> SessionAnalyticsEventRow {
        SessionAnalyticsEventRow {
            session_id: "session".to_string(),
            event_order,
            turn_seq: 1,
            event_unix_ms: i64::try_from(event_order * 100).unwrap(),
            event_class: event_class.to_string(),
            actor_role: String::new(),
            payload_type: String::new(),
            call_id: call_id.to_string(),
            tool_name: "Read".to_string(),
            tool_error: 0,
            latency_ms: 0,
            model: " CODEX ".to_string(),
            endpoint_kind: "generation".to_string(),
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            token_usage_buckets: BTreeMap::new(),
            token_usage_native_units: BTreeMap::new(),
            text_preview: String::new(),
            text_content: String::new(),
            tool_args_json: String::new(),
            harness: String::new(),
            source_name: String::new(),
            trace_id: String::new(),
        }
    }

    fn turn_row() -> TurnSummaryRow {
        TurnSummaryRow {
            session_id: "session".to_string(),
            turn_seq: 1,
            turn_id: "turn".to_string(),
            started_at: String::new(),
            started_at_unix_ms: 0,
            ended_at: String::new(),
            ended_at_unix_ms: 0,
            total_events: 99,
            user_messages: 0,
            assistant_messages: 0,
            tool_calls: 2,
            tool_results: 1,
            reasoning_items: 0,
        }
    }

    #[test]
    fn scalar_tokens_fill_canonical_buckets_only_when_map_is_empty() {
        let mut row = event(1, "message", "");
        row.input_tokens = 5;
        row.output_tokens = 3;
        assert_eq!(event_token_buckets(&row).get("input_text"), Some(&5));
        assert_eq!(event_token_buckets(&row).get("output_text"), Some(&3));

        row.token_usage_buckets.insert("reasoning".to_string(), 7);
        assert_eq!(event_token_buckets(&row).get("reasoning"), Some(&7));
        assert!(!event_token_buckets(&row).contains_key("input_text"));
    }

    #[test]
    fn unmatched_tool_call_preserves_call_latency_and_error() {
        let mut call = event(1, "tool_call", "call");
        call.latency_ms = 17;
        call.tool_error = 1;

        let turn = assemble_turn(turn_row(), vec![call]);
        assert!(matches!(
            &turn.steps[0],
            SessionStep::ToolCall {
                latency_ms: Some(17),
                is_error: true,
                result: None,
                ..
            }
        ));
    }

    #[test]
    fn tool_result_pairs_with_latest_unmatched_call_in_same_turn() {
        let first = event(1, "tool_call", "call");
        let second = event(2, "tool_call", "call");
        let mut result = event(3, "tool_result", "call");
        result.text_content = "done".to_string();

        let turn = assemble_turn(turn_row(), vec![result, first, second]);
        assert_eq!(turn.summary.total_events, 99);
        let results = turn
            .steps
            .iter()
            .filter_map(|step| match step {
                SessionStep::ToolCall { result, .. } => Some(result.as_ref()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(results[0].is_none());
        assert_eq!(results[1].expect("latest call result").text, "done");
    }

    #[test]
    fn analytics_cache_uses_six_distinct_range_slots_and_expires_after_ttl() {
        let indices = AnalyticsRange::ALL.map(analytics_range_index);
        assert_eq!(indices, [0, 1, 2, 3, 4, 5]);

        let now = Instant::now();
        let fresh = AnalyticsCacheEntry {
            snapshot: AnalyticsSnapshot::default(),
            fetched_at: now,
        };
        let stale = AnalyticsCacheEntry {
            snapshot: AnalyticsSnapshot::default(),
            fetched_at: now
                .checked_sub(ANALYTICS_CACHE_TTL + Duration::from_secs(1))
                .expect("test instant"),
        };
        assert!(fresh.is_fresh(now));
        assert!(!stale.is_fresh(now));
    }
}
