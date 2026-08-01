use super::*;

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
    pub(super) async fn analytics_series_impl(
        &self,
        range: AnalyticsRange,
    ) -> RepoResult<AnalyticsSnapshot> {
        let slot = &self.analytics_cache[analytics_range_index(range)];
        let mut entry = slot.lock().await;
        let now = Instant::now();
        let publication_token = publication_cache_key(&format!("analytics:{range:?}"));
        if let Some(publication_token) = publication_token.as_deref() {
            if let Some(cached) = entry
                .as_ref()
                .filter(|cached| cached.is_fresh(now, publication_token))
            {
                return Ok(cached.snapshot.clone());
            }
        }

        let snapshot = self.load_analytics_snapshot(range).await?;
        if let Some(publication_token) = publication_token {
            *entry = Some(AnalyticsCacheEntry {
                publication_token,
                snapshot: snapshot.clone(),
                fetched_at: Instant::now(),
            });
        }
        Ok(snapshot)
    }

    async fn load_analytics_snapshot(
        &self,
        range: AnalyticsRange,
    ) -> RepoResult<AnalyticsSnapshot> {
        let canonical_events = self.live_events_source();
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
        let canonical_events = self.live_events_source();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analytics_cache_uses_six_distinct_range_slots_and_expires_after_ttl() {
        let indices = AnalyticsRange::ALL.map(analytics_range_index);
        assert_eq!(indices, [0, 1, 2, 3, 4, 5]);

        let now = Instant::now();
        let fresh = AnalyticsCacheEntry {
            publication_token: "snapshot-a".to_string(),
            snapshot: AnalyticsSnapshot::default(),
            fetched_at: now,
        };
        let stale = AnalyticsCacheEntry {
            publication_token: "snapshot-a".to_string(),
            snapshot: AnalyticsSnapshot::default(),
            fetched_at: now
                .checked_sub(ANALYTICS_CACHE_TTL + Duration::from_secs(1))
                .expect("test instant"),
        };
        assert!(fresh.is_fresh(now, "snapshot-a"));
        assert!(!fresh.is_fresh(now, "snapshot-b"));
        assert!(!stale.is_fresh(now, "snapshot-a"));
    }
}
