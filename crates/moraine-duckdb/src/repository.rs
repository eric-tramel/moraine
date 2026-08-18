use std::collections::{BTreeMap, BTreeSet, HashSet};

use async_trait::async_trait;
use moraine_conversations::{
    decode_cursor, encode_cursor, AnalyticsConcurrencyPoint, AnalyticsRange, AnalyticsSnapshot,
    AnalyticsTokenPoint, AnalyticsTurnPoint, AnalyticsWindow, Conversation, ConversationCursor,
    ConversationDetailOptions, ConversationListFilter, ConversationListSort,
    ConversationRepository, ConversationSearchHit, ConversationSearchQuery,
    ConversationSearchResults, ConversationSearchStats, FileAttentionQuery, FileAttentionTouch,
    IngestHeartbeat, IngestHeartbeatRead, IngestStatusRead, McpEventOpen, McpEventType,
    McpOpenSnapshot, McpSessionListCursor, McpSessionListFilter, McpSessionListItem,
    McpSessionOpen, McpTurnCompact, McpTurnOpen, McpTurnRef, OpenContext, OpenEvent,
    OpenEventRequest, Page, PageRequest, RepoConfig, RepoError, RepoResult, SearchEventHit,
    SearchEventsQuery, SearchEventsResult, SearchEventsStats, SearchMcpEventHit,
    SearchMcpEventsQuery, SearchMcpEventsResult, SearchMcpEventsStats, SessionAnalytics,
    SessionAnalyticsQuery, SessionEventCursor, SessionEventsDirection, SessionEventsQuery,
    SessionLookback, SessionMetadata, SessionMetadataSearchHit, SessionMetadataSearchQuery,
    SessionMetadataSearchResults, SessionMetadataSearchStats, SessionStep, SessionTurn,
    StoreConnectionMetrics, StoreDiagnostics, StoreHealth, StoreProbe, TablePreview,
    TablePreviewQuery, TableSummaries, ToolResult, TraceEvent, Turn, TurnCursor, TurnListFilter,
    TurnSummary, WebSearchEvent,
};

use crate::model::{
    conversation_summary, group_sessions, group_turns, non_empty, number, session_metadata, string,
    turn_summary, StoredEvent,
};
use crate::DuckDbStore;
use sha2::{Digest, Sha256};

fn repo_error(error: impl std::fmt::Display) -> RepoError {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    if lower.contains("interrupt") || lower.contains("shutting down") {
        RepoError::cancelled(message)
    } else if lower.contains("pool exhausted") || lower.contains("busy") {
        RepoError::busy(message)
    } else if lower.contains("out of memory")
        || lower.contains("no space")
        || lower.contains("maximum temp")
    {
        RepoError::resource_exhausted(message)
    } else {
        RepoError::backend(message)
    }
}

fn conversation_filter_sig(filter: &ConversationListFilter) -> String {
    format!(
        "from={:?};to={:?};mode={};sort={}",
        filter.from_unix_ms,
        filter.to_unix_ms,
        filter.mode.map(|mode| mode.as_str()).unwrap_or("__none__"),
        filter.sort.as_str(),
    )
}

fn turn_filter_sig(session_id: &str, filter: &TurnListFilter) -> String {
    format!(
        "session={};from={:?};to={:?}",
        session_id, filter.from_turn_seq, filter.to_turn_seq
    )
}

fn session_events_filter_sig(query: &SessionEventsQuery) -> String {
    let kinds = query
        .event_kinds
        .as_deref()
        .map(|kinds| {
            kinds
                .iter()
                .map(|kind| kind.as_str())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_else(|| "__none__".to_string());
    format!(
        "session={};direction={};event_kinds={kinds}",
        query.session_id,
        query.direction.as_str()
    )
}

fn mcp_session_filter_sig(config: &RepoConfig, filter: &McpSessionListFilter) -> String {
    serde_json::to_string(&(
        filter.start_unix_ms,
        filter.end_unix_ms,
        filter.mode.map(|mode| mode.as_str()),
        filter.harness.as_deref(),
        filter.source_name.as_deref(),
        filter.sort.as_str(),
        config
            .session_scope
            .as_ref()
            .map(|scope| scope.roots.as_slice()),
    ))
    .expect("DuckDB list_sessions cursor signature is serializable")
}

fn validate_bounds(from: Option<i64>, to: Option<i64>) -> RepoResult<()> {
    if from.zip(to).is_some_and(|(from, to)| from >= to) {
        return Err(RepoError::invalid_argument(
            "from_unix_ms must be less than to_unix_ms",
        ));
    }
    Ok(())
}

fn terminal(events: &[StoredEvent]) -> (bool, Option<String>) {
    events
        .iter()
        .rev()
        .find(|event| {
            event.event_type() == "assistant_response"
                || event.trace.payload_type.contains("turn_completed")
                || event.trace.payload_type.contains("task_complete")
        })
        .map(|event| (true, Some(event.trace.event_uid.clone())))
        .unwrap_or((false, None))
}

fn session_visible(events: &[StoredEvent], config: &RepoConfig) -> bool {
    let Some(scope) = config.session_scope.as_ref() else {
        return true;
    };
    let origin = events.iter().find_map(|event| {
        non_empty(string(&event.row, "cwd")).or_else(|| {
            serde_json::from_str::<serde_json::Value>(&event.trace.payload_json)
                .ok()
                .and_then(|payload| {
                    non_empty(string(&payload, "cwd"))
                        .or_else(|| non_empty(string(&payload, "workspacePath")))
                })
        })
    });
    origin.is_some_and(|origin| {
        scope.roots.iter().any(|root| {
            origin == *root
                || origin
                    .strip_prefix(root)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    })
}

fn search_terms(value: &str, max_terms: usize) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut terms = Vec::new();
    for token in value
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|term| (2..=64).contains(&term.len()))
        .map(str::to_ascii_lowercase)
    {
        if seen.insert(token.clone()) {
            terms.push(token);
            if terms.len() >= max_terms {
                break;
            }
        }
    }
    terms
}

fn searchable_text(event: &StoredEvent) -> String {
    format!(
        "{} {} {}",
        event.trace.text_content,
        string(&event.row, "text_preview"),
        event.trace.payload_json
    )
    .to_ascii_lowercase()
}

fn token_counts(value: &str) -> BTreeMap<String, u16> {
    let mut counts = BTreeMap::new();
    for token in value
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|term| (2..=64).contains(&term.len()))
    {
        *counts.entry(token.to_ascii_lowercase()).or_insert(0) += 1;
    }
    counts
}

fn corpus_stats(events: &[StoredEvent], terms: &[String]) -> (u64, f64, BTreeMap<String, u64>) {
    let documents = events
        .iter()
        .map(|event| token_counts(&searchable_text(event)))
        .filter(|counts| !counts.is_empty())
        .collect::<Vec<_>>();
    let docs = documents.len() as u64;
    let total_length = documents
        .iter()
        .map(|counts| counts.values().copied().map(u64::from).sum::<u64>())
        .sum::<u64>();
    let average_length = if docs == 0 {
        1.0
    } else {
        (total_length as f64 / docs as f64).max(1.0)
    };
    let document_frequencies = terms
        .iter()
        .map(|term| {
            (
                term.clone(),
                documents
                    .iter()
                    .filter(|counts| counts.contains_key(term))
                    .count() as u64,
            )
        })
        .collect();
    (docs, average_length, document_frequencies)
}

fn bm25_score(
    counts: &BTreeMap<String, u16>,
    terms: &[String],
    docs: u64,
    average_length: f64,
    document_frequencies: &BTreeMap<String, u64>,
    k1: f64,
    b: f64,
) -> (f64, u64, u32) {
    let document_length = counts.values().copied().map(u32::from).sum::<u32>();
    let mut score = 0.0;
    let mut matched = 0;
    for term in terms {
        let Some(tf) = counts.get(term).copied() else {
            continue;
        };
        matched += 1;
        let df = document_frequencies.get(term).copied().unwrap_or_default();
        let n = docs.max(df) as f64;
        let idf = (1.0 + ((n - df as f64 + 0.5) / (df as f64 + 0.5))).ln();
        let tf = f64::from(tf);
        let norm = tf + k1 * (1.0 - b + b * (f64::from(document_length) / average_length.max(1.0)));
        if norm > 0.0 {
            score += idf.max(0.0) * tf * (k1 + 1.0) / norm;
        }
    }
    (score, matched, document_length)
}

fn search_event_kind_matches(event: &StoredEvent, kind: &str) -> bool {
    match kind {
        "message" => matches!(event.trace.event_class.as_str(), "message" | "event_msg"),
        "reasoning" => event.trace.event_class == "reasoning",
        "tool_call" => event.trace.event_class == "tool_call",
        "tool_result" => event.trace.event_class == "tool_result",
        _ => false,
    }
}

fn query_id(query: &str) -> String {
    format!("duckdb-{:x}", Sha256::digest(query.as_bytes()))
}

fn snapshot_generation(events: &[StoredEvent]) -> u64 {
    let mut digest = Sha256::new();
    for event in events {
        digest.update(event.trace.event_uid.as_bytes());
        digest.update(number(&event.row, "event_version").to_le_bytes());
        digest.update(number(&event.row, "source_generation").to_le_bytes());
    }
    u64::from_le_bytes(
        digest.finalize()[..8]
            .try_into()
            .expect("SHA-256 digest has at least eight bytes"),
    )
}

fn find_path_match(value: &serde_json::Value, rel: &str) -> Option<(String, String)> {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                if let Some(path) = value.as_str() {
                    let structured_key = matches!(
                        key.as_str(),
                        "path" | "file" | "file_path" | "filepath" | "target" | "directory"
                    );
                    if structured_key && (path == rel || path.ends_with(&format!("/{rel}"))) {
                        return Some(("path_suffix".to_string(), path.to_string()));
                    }
                    if matches!(key.as_str(), "command" | "cmd") && path.contains(rel) {
                        return Some(("shell_path".to_string(), String::new()));
                    }
                }
                if let Some(found) = find_path_match(value, rel) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(values) => {
            values.iter().find_map(|value| find_path_match(value, rel))
        }
        _ => None,
    }
}

fn worktree_root(path: &str, rel: &str) -> String {
    path.strip_suffix(rel)
        .map(|root| root.trim_end_matches('/').to_string())
        .filter(|root| root.starts_with('/'))
        .unwrap_or_default()
}

fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn analytics_range(range: AnalyticsRange) -> (u32, u32) {
    match range {
        AnalyticsRange::FifteenMinutes => (15 * 60, 60),
        AnalyticsRange::OneHour => (60 * 60, 5 * 60),
        AnalyticsRange::SixHours => (6 * 60 * 60, 15 * 60),
        AnalyticsRange::TwentyFourHours => (24 * 60 * 60, 60 * 60),
        AnalyticsRange::SevenDays => (7 * 24 * 60 * 60, 6 * 60 * 60),
        AnalyticsRange::ThirtyDays => (30 * 24 * 60 * 60, 24 * 60 * 60),
    }
}

fn lookback_seconds(lookback: SessionLookback) -> Option<u64> {
    match lookback {
        SessionLookback::OneHour => Some(60 * 60),
        SessionLookback::SixHours => Some(6 * 60 * 60),
        SessionLookback::TwentyFourHours => Some(24 * 60 * 60),
        SessionLookback::SevenDays => Some(7 * 24 * 60 * 60),
        SessionLookback::ThirtyDays => Some(30 * 24 * 60 * 60),
        SessionLookback::NinetyDays => Some(90 * 24 * 60 * 60),
        SessionLookback::All => None,
    }
}

fn turn_ref(events: &[StoredEvent]) -> Option<McpTurnRef> {
    let summary = turn_summary(events)?;
    Some(McpTurnRef {
        session_id: summary.session_id,
        turn_seq: summary.turn_seq,
        turn_id: summary.turn_id,
        started_at: summary.started_at,
        ended_at: summary.ended_at,
    })
}

fn compact_turn(events: &[StoredEvent]) -> Option<McpTurnCompact> {
    let metadata = turn_summary(events)?;
    let first_event = events.first().map(StoredEvent::event_ref);
    let last_event = events.last().map(StoredEvent::event_ref);
    let user = events
        .iter()
        .find(|event| event.event_type() == "user_input");
    let assistant = events
        .iter()
        .rev()
        .find(|event| event.event_type() == "assistant_response");
    let (completed, terminal_event_uid) = terminal(events);
    Some(McpTurnCompact {
        metadata,
        user_input_summary: user.and_then(|event| non_empty(event.trace.text_content.clone())),
        final_response_summary: assistant
            .and_then(|event| non_empty(event.trace.text_content.clone())),
        user_input_event: user.map(StoredEvent::event_ref),
        final_response_event: assistant.map(StoredEvent::event_ref),
        tools_called: events
            .iter()
            .filter(|event| event.trace.event_class == "tool_call")
            .map(|event| event.trace.name.clone())
            .filter(|name| !name.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        normalized_event_types: events
            .iter()
            .map(|event| event.event_type().to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        completed,
        terminal_event_uid,
        first_event,
        last_event,
    })
}

impl DuckDbStore {
    async fn stored_events(&self, session_id: Option<&str>) -> RepoResult<Vec<StoredEvent>> {
        let values = self
            .current_rows(session_id.map(str::to_string), None)
            .await
            .map_err(repo_error)?;
        Ok(values.into_iter().map(StoredEvent::from_row).collect())
    }
}

#[async_trait]
impl ConversationRepository for DuckDbStore {
    fn config(&self) -> &RepoConfig {
        &self.repository_config
    }

    async fn prewarm_mcp_search_state(&self) -> RepoResult<()> {
        Ok(())
    }

    async fn list_session_analytics(
        &self,
        query: SessionAnalyticsQuery,
    ) -> RepoResult<Vec<SessionAnalytics>> {
        let cutoff = lookback_seconds(query.lookback)
            .map(|window| now_unix_seconds().saturating_sub(window) as i64 * 1_000);
        let mut sessions = group_sessions(self.stored_events(None).await?)
            .into_values()
            .filter_map(|events| {
                let summary = conversation_summary(&events)?;
                if cutoff.is_some_and(|cutoff| summary.last_event_unix_ms < cutoff) {
                    return None;
                }
                let first = events.first()?;
                let mut models = events
                    .iter()
                    .map(|event| event.model.clone())
                    .filter(|model| !model.is_empty())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                models.sort();
                let first_user_text = events
                    .iter()
                    .find(|event| event.event_type() == "user_input")
                    .map(|event| event.trace.text_content.clone())
                    .unwrap_or_default();
                let turns = group_turns(&events)
                    .into_values()
                    .filter_map(|turn_events| {
                        let summary = turn_summary(&turn_events)?;
                        let model = turn_events
                            .iter()
                            .find_map(|event| non_empty(event.model.clone()))
                            .unwrap_or_default();
                        let mut usage = BTreeMap::new();
                        let mut steps = Vec::new();
                        for event in &turn_events {
                            for (bucket, tokens) in &event.trace.token_usage_buckets {
                                *usage.entry(bucket.clone()).or_insert(0) += tokens;
                            }
                            let step = match event.event_type() {
                                "user_input" => Some(SessionStep::User {
                                    event_unix_ms: event.trace.event_unix_ms,
                                    text: event.trace.text_content.clone(),
                                }),
                                "assistant_response" => Some(SessionStep::Assistant {
                                    event_unix_ms: event.trace.event_unix_ms,
                                    text: event.trace.text_content.clone(),
                                    endpoint_kind: event.trace.endpoint_kind.clone(),
                                    latency_ms: (number(&event.row, "latency_ms") > 0).then(|| {
                                        number(&event.row, "latency_ms").min(u64::from(u32::MAX))
                                            as u32
                                    }),
                                    token_usage_buckets: event.trace.token_usage_buckets.clone(),
                                    token_usage_native_units: event
                                        .trace
                                        .token_usage_native_units
                                        .clone(),
                                }),
                                "reasoning" => Some(SessionStep::Thinking {
                                    event_unix_ms: event.trace.event_unix_ms,
                                    text: event.trace.text_content.clone(),
                                }),
                                "tool_call" => {
                                    let result = turn_events.iter().find(|candidate| {
                                        candidate.trace.event_class == "tool_result"
                                            && !event.trace.call_id.is_empty()
                                            && candidate.trace.call_id == event.trace.call_id
                                    });
                                    Some(SessionStep::ToolCall {
                                        event_unix_ms: event.trace.event_unix_ms,
                                        tool_name: event.trace.name.clone(),
                                        call_id: event.trace.call_id.clone(),
                                        arguments: serde_json::from_str(&event.trace.payload_json)
                                            .unwrap_or(serde_json::Value::Null),
                                        latency_ms: (number(&event.row, "latency_ms") > 0).then(
                                            || {
                                                number(&event.row, "latency_ms")
                                                    .min(u64::from(u32::MAX))
                                                    as u32
                                            },
                                        ),
                                        is_error: number(&event.row, "tool_error") != 0,
                                        result: result.map(|result| ToolResult {
                                            event_unix_ms: result.trace.event_unix_ms,
                                            text: result.trace.text_content.clone(),
                                            latency_ms: number(&result.row, "latency_ms")
                                                .min(u64::from(u32::MAX))
                                                as u32,
                                            is_error: number(&result.row, "tool_error") != 0,
                                        }),
                                    })
                                }
                                _ => None,
                            };
                            if let Some(step) = step {
                                steps.push(step);
                            }
                        }
                        Some(SessionTurn {
                            summary,
                            model,
                            token_usage_buckets: usage,
                            steps,
                        })
                    })
                    .collect();
                Some(SessionAnalytics {
                    summary: summary.clone(),
                    harness: first.harness.clone(),
                    source_name: first.source_name.clone(),
                    models,
                    trace_id: string(&first.row, "trace_id"),
                    first_user_text: first_user_text.clone(),
                    display_label: first_user_text
                        .lines()
                        .next()
                        .unwrap_or(&summary.session_id)
                        .chars()
                        .take(120)
                        .collect(),
                    turns,
                })
            })
            .collect::<Vec<_>>();
        sessions.sort_by(|left, right| {
            right
                .summary
                .last_event_unix_ms
                .cmp(&left.summary.last_event_unix_ms)
                .then_with(|| left.summary.session_id.cmp(&right.summary.session_id))
        });
        sessions.truncate(usize::from(query.limit.clamp(1, 200)));
        Ok(sessions)
    }

    async fn analytics_series(&self, range: AnalyticsRange) -> RepoResult<AnalyticsSnapshot> {
        let (window_seconds, bucket_seconds) = analytics_range(range);
        let to_unix = now_unix_seconds();
        let from_unix = to_unix.saturating_sub(u64::from(window_seconds));
        let events = self
            .stored_events(None)
            .await?
            .into_iter()
            .filter(|event| event.trace.event_unix_ms >= from_unix as i64 * 1_000)
            .collect::<Vec<_>>();
        let bucket = |unix_ms: i64| {
            let seconds = unix_ms.max(0) as u64 / 1_000;
            seconds / u64::from(bucket_seconds) * u64::from(bucket_seconds)
        };
        let mut token_totals = BTreeMap::<(u64, String, String, String), u64>::new();
        for event in &events {
            for (name, tokens) in &event.trace.token_usage_buckets {
                if *tokens > 0 {
                    *token_totals
                        .entry((
                            bucket(event.trace.event_unix_ms),
                            event.model.clone(),
                            event.trace.endpoint_kind.clone(),
                            name.clone(),
                        ))
                        .or_insert(0) += tokens;
                }
            }
        }
        let tokens = token_totals
            .into_iter()
            .map(
                |((bucket_unix, model, endpoint_kind, bucket), tokens)| AnalyticsTokenPoint {
                    bucket_unix,
                    model,
                    endpoint_kind,
                    bucket,
                    tokens,
                },
            )
            .collect();
        let mut turn_totals = BTreeMap::<(u64, String), u64>::new();
        for session in group_sessions(events.clone()).into_values() {
            for turn in group_turns(&session).into_values() {
                if let Some(first) = turn.first() {
                    *turn_totals
                        .entry((bucket(first.trace.event_unix_ms), first.model.clone()))
                        .or_insert(0) += 1;
                }
            }
        }
        let turns = turn_totals
            .into_iter()
            .map(|((bucket_unix, model), turns)| AnalyticsTurnPoint {
                bucket_unix,
                model,
                turns,
            })
            .collect();
        let sessions = group_sessions(events)
            .into_values()
            .filter_map(|events| conversation_summary(&events))
            .collect::<Vec<_>>();
        let first_bucket = from_unix / u64::from(bucket_seconds) * u64::from(bucket_seconds);
        let concurrent_sessions = (first_bucket..=to_unix)
            .step_by(bucket_seconds as usize)
            .map(|bucket_unix| AnalyticsConcurrencyPoint {
                bucket_unix,
                concurrent_sessions: sessions
                    .iter()
                    .filter(|session| {
                        session.first_event_unix_ms
                            < (bucket_unix + u64::from(bucket_seconds)) as i64 * 1_000
                            && session.last_event_unix_ms >= bucket_unix as i64 * 1_000
                    })
                    .count() as u64,
            })
            .collect();
        Ok(AnalyticsSnapshot {
            window: AnalyticsWindow {
                range,
                window_seconds,
                bucket_seconds,
                from_unix,
                to_unix,
            },
            tokens,
            turns,
            concurrent_sessions,
        })
    }

    async fn list_web_searches(&self, limit: u16) -> RepoResult<Vec<WebSearchEvent>> {
        let mut rows = self
            .stored_events(None)
            .await?
            .into_iter()
            .filter(|event| {
                event.trace.name.eq_ignore_ascii_case("web_search")
                    || event.trace.payload_type.contains("web_search")
            })
            .map(|event| {
                let payload = serde_json::from_str::<serde_json::Value>(&event.trace.payload_json)
                    .unwrap_or(serde_json::Value::Null);
                WebSearchEvent {
                    event_time: event.trace.event_time,
                    harness: event.harness,
                    source_name: event.source_name,
                    session_id: event.trace.session_id,
                    model: event.model,
                    action: non_empty(string(&payload, "action"))
                        .unwrap_or_else(|| event.trace.phase.clone()),
                    search_query: non_empty(string(&payload, "query"))
                        .or_else(|| non_empty(string(&payload, "search_query")))
                        .unwrap_or_default(),
                    result_url: non_empty(string(&payload, "url"))
                        .or_else(|| non_empty(string(&payload, "result_url")))
                        .unwrap_or_default(),
                    source_ref: event.trace.source_ref,
                }
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| right.event_time.cmp(&left.event_time));
        rows.truncate(usize::from(limit.clamp(1, 500)));
        Ok(rows)
    }

    async fn latest_ingest_heartbeat(&self) -> RepoResult<IngestHeartbeatRead> {
        let latest = self
            .ingest_heartbeat_values(1)
            .await
            .map_err(repo_error)?
            .into_iter()
            .next()
            .map(serde_json::from_value)
            .transpose()
            .map_err(repo_error)?;
        Ok(IngestHeartbeatRead {
            table_present: true,
            latest,
        })
    }

    async fn ingest_status(&self, history_limit: u16) -> RepoResult<IngestStatusRead> {
        let mut history = self
            .ingest_heartbeat_values(history_limit.min(120).saturating_add(1))
            .await
            .map_err(repo_error)?
            .into_iter()
            .map(serde_json::from_value::<IngestHeartbeat>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(repo_error)?;
        let latest = history.first().cloned();
        history.reverse();
        Ok(IngestStatusRead {
            heartbeat: IngestHeartbeatRead {
                table_present: true,
                latest,
            },
            history,
        })
    }

    async fn list_table_summaries(&self) -> RepoResult<TableSummaries> {
        self.table_summaries().await.map_err(repo_error)
    }

    async fn preview_table(&self, query: TablePreviewQuery) -> RepoResult<TablePreview> {
        let limit = query.limit.clamp(1, 500);
        self.preview_table_rows(&query.table, limit)
            .await
            .map_err(repo_error)
    }

    async fn read_store_health(&self) -> RepoResult<StoreHealth> {
        let connections = self.reader_count().saturating_add(1) as u64;
        Ok(StoreHealth {
            ping: StoreProbe::Available(0.0),
            version: StoreProbe::Available(self.engine_version.clone()),
            database_exists: StoreProbe::Available(true),
            connections: StoreProbe::Available(StoreConnectionMetrics {
                total: connections,
                ..StoreConnectionMetrics::default()
            }),
        })
    }

    async fn read_store_diagnostics(&self) -> RepoResult<StoreDiagnostics> {
        Ok(StoreDiagnostics {
            healthy: !self.is_shutting_down(),
            version: Some(self.engine_version.clone()),
            database: "default (duckdb local)".to_string(),
            database_exists: true,
            applied_schema_versions: self
                .applied_migrations()
                .iter()
                .map(|migration| migration.filename.clone())
                .collect(),
            pending_schema_versions: Vec::new(),
            missing_tables: Vec::new(),
            errors: Vec::new(),
        })
    }

    async fn list_conversations(
        &self,
        filter: ConversationListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<moraine_conversations::ConversationSummary>> {
        validate_bounds(filter.from_unix_ms, filter.to_unix_ms)?;
        let signature = conversation_filter_sig(&filter);
        let cursor = page
            .cursor
            .as_deref()
            .map(decode_cursor::<ConversationCursor>)
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.filter_sig != signature || cursor.sort != filter.sort)
        {
            return Err(RepoError::invalid_cursor(
                "cursor does not match current conversation filter",
            ));
        }
        let mut items = group_sessions(self.stored_events(None).await?)
            .into_values()
            .filter_map(|events| conversation_summary(&events))
            .filter(|summary| {
                filter
                    .from_unix_ms
                    .is_none_or(|from| summary.last_event_unix_ms >= from)
                    && filter
                        .to_unix_ms
                        .is_none_or(|to| summary.last_event_unix_ms < to)
                    && filter.mode.is_none_or(|mode| summary.mode == mode)
            })
            .filter(|summary| {
                cursor.as_ref().is_none_or(|cursor| {
                    let key = (summary.last_event_unix_ms, summary.session_id.as_str());
                    let cursor_key = (cursor.last_event_unix_ms, cursor.session_id.as_str());
                    match filter.sort {
                        ConversationListSort::Asc => key > cursor_key,
                        ConversationListSort::Desc => key < cursor_key,
                    }
                })
            })
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            let order = left
                .last_event_unix_ms
                .cmp(&right.last_event_unix_ms)
                .then_with(|| left.session_id.cmp(&right.session_id));
            match filter.sort {
                ConversationListSort::Asc => order,
                ConversationListSort::Desc => order.reverse(),
            }
        });
        let limit = usize::from(page.normalized_limit(self.config().max_results));
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if has_more {
            items
                .last()
                .map(|last| {
                    encode_cursor(&ConversationCursor {
                        last_event_unix_ms: last.last_event_unix_ms,
                        session_id: last.session_id.clone(),
                        filter_sig: signature,
                        sort: filter.sort,
                    })
                })
                .transpose()?
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    async fn get_conversation(
        &self,
        session_id: &str,
        options: ConversationDetailOptions,
    ) -> RepoResult<Option<Conversation>> {
        let events = self.stored_events(Some(session_id)).await?;
        let Some(summary) = conversation_summary(&events) else {
            return Ok(None);
        };
        let turns = if options.include_turns {
            group_turns(&events)
                .into_values()
                .filter_map(|events| turn_summary(&events))
                .collect()
        } else {
            Vec::new()
        };
        Ok(Some(Conversation { summary, turns }))
    }

    async fn get_session_metadata(&self, session_id: &str) -> RepoResult<Option<SessionMetadata>> {
        let events = self.stored_events(Some(session_id)).await?;
        if !session_visible(&events, self.config()) {
            return Ok(None);
        }
        Ok(session_metadata(&events))
    }

    async fn get_mcp_session(&self, session_id: &str) -> RepoResult<Option<McpSessionOpen>> {
        let events = self.stored_events(Some(session_id)).await?;
        if !session_visible(&events, self.config()) {
            return Ok(None);
        }
        let Some(metadata) = session_metadata(&events) else {
            return Ok(None);
        };
        let first = events.first().expect("metadata requires an event");
        let summary = conversation_summary(&events).expect("metadata requires summary");
        let (completed, terminal_event_uid) = terminal(&events);
        let generation = snapshot_generation(&events);
        let turns = group_turns(&events)
            .into_values()
            .filter_map(|events| compact_turn(&events))
            .collect();
        Ok(Some(McpSessionOpen {
            metadata,
            title: summary.session_summary.clone(),
            source: non_empty(first.source_name.clone()),
            harness: non_empty(first.harness.clone()),
            inference_provider: non_empty(first.inference_provider.clone()),
            session_slug: summary.session_slug,
            session_summary: summary.session_summary,
            turns,
            completed,
            terminal_event_uid,
            snapshot: Some(McpOpenSnapshot {
                slot: 0,
                generation,
            }),
        }))
    }

    async fn list_mcp_sessions(
        &self,
        filter: McpSessionListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<McpSessionListItem>> {
        validate_bounds(Some(filter.start_unix_ms), Some(filter.end_unix_ms))?;
        let signature = mcp_session_filter_sig(self.config(), &filter);
        let cursor = page
            .cursor
            .as_deref()
            .map(decode_cursor::<McpSessionListCursor>)
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.filter_sig != signature || cursor.sort != filter.sort)
        {
            return Err(RepoError::invalid_cursor(
                "cursor does not match current session filter",
            ));
        }
        let mut items = group_sessions(self.stored_events(None).await?)
            .into_values()
            .filter(|events| session_visible(events, self.config()))
            .filter_map(|events| {
                let summary = conversation_summary(&events)?;
                let first = events.first()?;
                let (completed, _) = terminal(&events);
                if summary.last_event_unix_ms < filter.start_unix_ms
                    || summary.last_event_unix_ms >= filter.end_unix_ms
                    || filter.mode.is_some_and(|mode| summary.mode != mode)
                    || filter
                        .harness
                        .as_deref()
                        .is_some_and(|harness| first.harness != harness)
                    || filter
                        .source_name
                        .as_deref()
                        .is_some_and(|source| first.source_name != source)
                {
                    return None;
                }
                Some(McpSessionListItem {
                    session_id: summary.session_id.clone(),
                    first_event_time: summary.first_event_time,
                    first_event_unix_ms: summary.first_event_unix_ms,
                    last_event_time: summary.last_event_time,
                    last_event_unix_ms: summary.last_event_unix_ms,
                    total_turns: summary.total_turns,
                    total_events: summary.total_events,
                    mode: summary.mode,
                    completed,
                    title: summary.session_summary.clone(),
                    source: non_empty(first.source_name.clone()),
                    harness: non_empty(first.harness.clone()),
                    session_slug: summary.session_slug,
                    session_summary: summary.session_summary,
                    display_label: first
                        .trace
                        .text_content
                        .lines()
                        .next()
                        .unwrap_or(&summary.session_id)
                        .chars()
                        .take(120)
                        .collect(),
                })
            })
            .filter(|item| {
                cursor.as_ref().is_none_or(|cursor| {
                    let key = (item.last_event_unix_ms, item.session_id.as_str());
                    let cursor_key = (cursor.last_event_unix_ms, cursor.session_id.as_str());
                    match filter.sort {
                        ConversationListSort::Asc => key > cursor_key,
                        ConversationListSort::Desc => key < cursor_key,
                    }
                })
            })
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            let order = left
                .last_event_unix_ms
                .cmp(&right.last_event_unix_ms)
                .then_with(|| left.session_id.cmp(&right.session_id));
            match filter.sort {
                ConversationListSort::Asc => order,
                ConversationListSort::Desc => order.reverse(),
            }
        });
        let limit = usize::from(page.normalized_limit(self.config().max_results));
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if has_more {
            items
                .last()
                .map(|last| {
                    encode_cursor(&McpSessionListCursor {
                        last_event_unix_ms: last.last_event_unix_ms,
                        session_id: last.session_id.clone(),
                        filter_sig: signature,
                        sort: filter.sort,
                    })
                })
                .transpose()?
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    async fn list_turns(
        &self,
        session_id: &str,
        filter: TurnListFilter,
        page: PageRequest,
    ) -> RepoResult<Page<TurnSummary>> {
        let signature = turn_filter_sig(session_id, &filter);
        let cursor = page
            .cursor
            .as_deref()
            .map(decode_cursor::<TurnCursor>)
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.session_id != session_id || cursor.filter_sig != signature)
        {
            return Err(RepoError::invalid_cursor(
                "cursor does not match current turn filter",
            ));
        }
        let session_events = self.stored_events(Some(session_id)).await?;
        let publication_token = Some(snapshot_generation(&session_events).to_string());
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.publication_token.is_some() && cursor.publication_token != publication_token
        }) {
            return Err(RepoError::ReadModelChanged);
        }
        let mut items = group_turns(&session_events)
            .into_values()
            .filter_map(|events| turn_summary(&events))
            .filter(|turn| {
                filter
                    .from_turn_seq
                    .is_none_or(|from| turn.turn_seq >= from)
                    && filter.to_turn_seq.is_none_or(|to| turn.turn_seq <= to)
                    && cursor
                        .as_ref()
                        .is_none_or(|cursor| turn.turn_seq > cursor.last_turn_seq)
            })
            .collect::<Vec<_>>();
        items.sort_by_key(|turn| turn.turn_seq);
        let limit = usize::from(page.normalized_limit(self.config().max_results));
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if has_more {
            items
                .last()
                .map(|last| {
                    encode_cursor(&TurnCursor {
                        last_turn_seq: last.turn_seq,
                        session_id: session_id.to_string(),
                        filter_sig: signature,
                        publication_token: publication_token.clone(),
                    })
                })
                .transpose()?
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    async fn get_turn(&self, session_id: &str, turn_seq: u32) -> RepoResult<Option<Turn>> {
        let events = self
            .stored_events(Some(session_id))
            .await?
            .into_iter()
            .filter(|event| event.trace.turn_seq == turn_seq)
            .collect::<Vec<_>>();
        let Some(summary) = turn_summary(&events) else {
            return Ok(None);
        };
        Ok(Some(Turn {
            summary,
            events: events.into_iter().map(|event| event.trace).collect(),
        }))
    }

    async fn get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
    ) -> RepoResult<Option<McpTurnOpen>> {
        let session_events = self.stored_events(Some(session_id)).await?;
        if !session_visible(&session_events, self.config()) {
            return Ok(None);
        }
        let turns = group_turns(&session_events);
        let Some(events) = turns.get(&turn_seq) else {
            return Ok(None);
        };
        let Some(compact) = compact_turn(events) else {
            return Ok(None);
        };
        let previous_turn = turns
            .range(..turn_seq)
            .next_back()
            .and_then(|(_, events)| turn_ref(events));
        let next_turn = turns
            .range(turn_seq.saturating_add(1)..)
            .next()
            .and_then(|(_, events)| turn_ref(events));
        let source = session_events
            .first()
            .and_then(|event| non_empty(event.source_name.clone()));
        let generation = snapshot_generation(&session_events);
        Ok(Some(McpTurnOpen {
            metadata: compact.metadata,
            events: events.iter().map(StoredEvent::event_summary).collect(),
            parent_session_source: source,
            user_input_summary: compact.user_input_summary,
            final_response_summary: compact.final_response_summary,
            user_input_event: compact.user_input_event,
            final_response_event: compact.final_response_event,
            tools_called: compact.tools_called,
            normalized_event_types: compact.normalized_event_types,
            completed: compact.completed,
            terminal_event_uid: compact.terminal_event_uid,
            previous_turn,
            next_turn,
            first_event: compact.first_event,
            last_event: compact.last_event,
            snapshot: Some(McpOpenSnapshot {
                slot: 0,
                generation,
            }),
        }))
    }

    async fn open_event(&self, request: OpenEventRequest) -> RepoResult<OpenContext> {
        let before = request
            .before
            .unwrap_or(self.config().default_context_before);
        let after = request.after.unwrap_or(self.config().default_context_after);
        let target = self
            .current_rows(None, Some(request.event_uid.clone()))
            .await
            .map_err(repo_error)?
            .into_iter()
            .next()
            .map(StoredEvent::from_row);
        let Some(target) = target else {
            return Ok(OpenContext {
                found: false,
                event_uid: request.event_uid,
                session_id: String::new(),
                target_event_order: 0,
                turn_seq: 0,
                before,
                after,
                events: Vec::new(),
            });
        };
        let all = self.stored_events(Some(&target.trace.session_id)).await?;
        let position = all
            .iter()
            .position(|event| event.trace.event_uid == target.trace.event_uid)
            .expect("target event is part of its session");
        let start = position.saturating_sub(usize::from(before));
        let end = position
            .saturating_add(usize::from(after))
            .saturating_add(1)
            .min(all.len());
        let events = all[start..end]
            .iter()
            .filter(|event| {
                request.include_system_events.unwrap_or(false)
                    || !event.trace.actor_role.eq_ignore_ascii_case("system")
                    || event.trace.event_uid == target.trace.event_uid
            })
            .map(|event| OpenEvent {
                is_target: event.trace.event_uid == target.trace.event_uid,
                session_id: event.trace.session_id.clone(),
                event_uid: event.trace.event_uid.clone(),
                event_order: event.trace.event_order,
                turn_seq: event.trace.turn_seq,
                event_time: event.trace.event_time.clone(),
                actor_role: event.trace.actor_role.clone(),
                event_class: event.trace.event_class.clone(),
                payload_type: event.trace.payload_type.clone(),
                call_id: event.trace.call_id.clone(),
                name: event.trace.name.clone(),
                phase: event.trace.phase.clone(),
                item_id: event.trace.item_id.clone(),
                source_ref: event.trace.source_ref.clone(),
                text_content: event.trace.text_content.clone(),
                payload_json: event.trace.payload_json.clone(),
                token_usage_json: event.trace.token_usage_json.clone(),
                endpoint_kind: event.trace.endpoint_kind.clone(),
                token_usage_buckets: event.trace.token_usage_buckets.clone(),
                token_usage_native_units: event.trace.token_usage_native_units.clone(),
            })
            .collect();
        Ok(OpenContext {
            found: true,
            event_uid: target.trace.event_uid,
            session_id: target.trace.session_id,
            target_event_order: target.trace.event_order,
            turn_seq: target.trace.turn_seq,
            before,
            after,
            events,
        })
    }

    async fn get_mcp_event(&self, event_uid: &str) -> RepoResult<Option<McpEventOpen>> {
        let target = self
            .current_rows(None, Some(event_uid.to_string()))
            .await
            .map_err(repo_error)?
            .into_iter()
            .next()
            .map(StoredEvent::from_row);
        let Some(target) = target else {
            return Ok(None);
        };
        let all = self.stored_events(Some(&target.trace.session_id)).await?;
        if !session_visible(&all, self.config()) {
            return Ok(None);
        }
        let turns = group_turns(&all);
        let turn_events = turns
            .get(&target.trace.turn_seq)
            .expect("target turn exists");
        let event_position = turn_events
            .iter()
            .position(|event| event.trace.event_uid == target.trace.event_uid)
            .expect("target event exists in target turn");
        let (turn_completed, turn_terminal_event_uid) = terminal(turn_events);
        let parent_session = session_metadata(&all).expect("target session exists");
        let parent_turn = turn_summary(turn_events).expect("target turn exists");
        Ok(Some(McpEventOpen {
            event: target.trace.clone(),
            event_type: target.event_type().to_string(),
            event_ordinal: event_position as u32 + 1,
            turn_completed,
            turn_terminal_event_uid,
            parent_session,
            parent_session_source: non_empty(target.source_name.clone()),
            parent_turn,
            previous_event: event_position
                .checked_sub(1)
                .and_then(|index| turn_events.get(index))
                .map(StoredEvent::event_ref),
            next_event: turn_events
                .get(event_position.saturating_add(1))
                .map(StoredEvent::event_ref),
            previous_turn: turns
                .range(..target.trace.turn_seq)
                .next_back()
                .and_then(|(_, events)| turn_ref(events)),
            next_turn: turns
                .range(target.trace.turn_seq.saturating_add(1)..)
                .next()
                .and_then(|(_, events)| turn_ref(events)),
        }))
    }

    async fn list_session_events(
        &self,
        query: SessionEventsQuery,
        page: PageRequest,
    ) -> RepoResult<Page<TraceEvent>> {
        let signature = session_events_filter_sig(&query);
        let cursor = page
            .cursor
            .as_deref()
            .map(decode_cursor::<SessionEventCursor>)
            .transpose()?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.session_id != query.session_id
                || cursor.direction != query.direction
                || cursor.filter_sig != signature
        }) {
            return Err(RepoError::invalid_cursor(
                "cursor does not match current event filter",
            ));
        }
        let session_events = self.stored_events(Some(&query.session_id)).await?;
        let publication_token = Some(snapshot_generation(&session_events).to_string());
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.publication_token.is_some() && cursor.publication_token != publication_token
        }) {
            return Err(RepoError::ReadModelChanged);
        }
        let mut events = session_events
            .into_iter()
            .filter(|event| {
                query.event_kinds.as_ref().is_none_or(|kinds| {
                    kinds
                        .iter()
                        .any(|kind| search_event_kind_matches(event, kind.as_str()))
                })
            })
            .filter(|event| {
                cursor.as_ref().is_none_or(|cursor| {
                    let key = (event.trace.event_order, event.trace.event_uid.as_str());
                    let cursor_key = (cursor.last_event_order, cursor.last_event_uid.as_str());
                    match query.direction {
                        SessionEventsDirection::Forward => key > cursor_key,
                        SessionEventsDirection::Reverse => key < cursor_key,
                    }
                })
            })
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            let order = left
                .trace
                .event_order
                .cmp(&right.trace.event_order)
                .then_with(|| left.trace.event_uid.cmp(&right.trace.event_uid));
            match query.direction {
                SessionEventsDirection::Forward => order,
                SessionEventsDirection::Reverse => order.reverse(),
            }
        });
        let limit = usize::from(page.normalized_limit(self.config().max_results));
        let has_more = events.len() > limit;
        events.truncate(limit);
        let next_cursor = if has_more {
            events
                .last()
                .map(|last| {
                    encode_cursor(&SessionEventCursor {
                        last_event_order: last.trace.event_order,
                        last_event_uid: last.trace.event_uid.clone(),
                        session_id: query.session_id,
                        direction: query.direction,
                        filter_sig: signature,
                        publication_token: publication_token.clone(),
                    })
                })
                .transpose()?
        } else {
            None
        };
        Ok(Page {
            items: events.into_iter().map(|event| event.trace).collect(),
            next_cursor,
        })
    }

    async fn search_events(&self, query: SearchEventsQuery) -> RepoResult<SearchEventsResult> {
        let terms = search_terms(&query.query, self.config().bm25_max_query_terms);
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "query must contain at least one searchable term",
            ));
        }
        let requested_limit = query.limit.unwrap_or(self.config().max_results).max(1);
        let effective_limit = requested_limit.min(self.config().max_results);
        let min_should_match = usize::from(
            query
                .min_should_match
                .unwrap_or(self.config().bm25_default_min_should_match)
                .max(1)
                .min(terms.len() as u16),
        );
        let minimum_score = query
            .min_score
            .unwrap_or(self.config().bm25_default_min_score);
        let include_tool_events = query
            .include_tool_events
            .unwrap_or(self.config().default_include_tool_events);
        let exclude_codex_mcp = query
            .exclude_codex_mcp
            .unwrap_or(self.config().default_exclude_codex_mcp);
        let all = self.stored_events(None).await?;
        let (docs, average_length, document_frequencies) = corpus_stats(&all, &terms);
        let k1 = self.config().bm25_k1.max(0.01);
        let b = self.config().bm25_b.clamp(0.0, 1.0);
        let mut scored = all
            .into_iter()
            .filter(|event| {
                query
                    .session_id
                    .as_deref()
                    .is_none_or(|session| event.trace.session_id == session)
                    && query.session_ids.as_ref().is_none_or(|sessions| {
                        sessions
                            .iter()
                            .any(|session| session == &event.trace.session_id)
                    })
                    && query.event_kinds.as_ref().is_none_or(|kinds| {
                        kinds
                            .iter()
                            .any(|kind| search_event_kind_matches(event, kind.as_str()))
                    })
                    && (include_tool_events
                        || !matches!(
                            event.trace.event_class.as_str(),
                            "tool_call" | "tool_result"
                        ))
                    && (!exclude_codex_mcp || event.source_name != "codex-mcp")
            })
            .filter_map(|event| {
                let counts = token_counts(&searchable_text(&event));
                let (score, matched_terms, doc_len) = bm25_score(
                    &counts,
                    &terms,
                    docs,
                    average_length,
                    &document_frequencies,
                    k1,
                    b,
                );
                if matched_terms < min_should_match as u64 || score < minimum_score {
                    return None;
                }
                Some((event, score, matched_terms, doc_len))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|(left, left_score, _, _), (right, right_score, _, _)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| right.trace.event_unix_ms.cmp(&left.trace.event_unix_ms))
                .then_with(|| left.trace.event_uid.cmp(&right.trace.event_uid))
        });
        scored.truncate(usize::from(effective_limit));
        let hits = scored
            .into_iter()
            .enumerate()
            .map(
                |(index, (event, score, matched_terms, doc_len))| SearchEventHit {
                    rank: index + 1,
                    event_uid: event.trace.event_uid,
                    session_id: event.trace.session_id,
                    source_host: string(&event.row, "source_host"),
                    event_time: non_empty(event.trace.event_time.clone()),
                    first_event_time: event.trace.event_time.clone(),
                    last_event_time: event.trace.event_time.clone(),
                    source_name: event.source_name,
                    harness: event.harness,
                    inference_provider: event.inference_provider,
                    score,
                    matched_terms,
                    doc_len,
                    event_class: event.trace.event_class,
                    payload_type: event.trace.payload_type,
                    actor_role: event.trace.actor_role,
                    name: event.trace.name,
                    phase: event.trace.phase,
                    source_ref: event.trace.source_ref,
                    text_preview: string(&event.row, "text_preview"),
                    text_content: non_empty(event.trace.text_content),
                    payload_json: non_empty(event.trace.payload_json),
                },
            )
            .collect::<Vec<_>>();
        Ok(SearchEventsResult {
            query_id: query_id(&query.query),
            query: query.query,
            terms,
            stats: SearchEventsStats {
                docs,
                avgdl: average_length,
                took_ms: 0,
                result_count: hits.len(),
                requested_limit,
                effective_limit,
                limit_capped: requested_limit > effective_limit,
            },
            hits,
        })
    }

    async fn search_mcp_events(
        &self,
        query: SearchMcpEventsQuery,
    ) -> RepoResult<SearchMcpEventsResult> {
        let terms = search_terms(&query.query, self.config().bm25_max_query_terms);
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "query must contain at least one searchable term",
            ));
        }
        let event_types = query
            .event_types
            .clone()
            .unwrap_or_else(McpEventType::default_search_types);
        if event_types.is_empty() {
            return Err(RepoError::invalid_argument(
                "event_types filter cannot be empty",
            ));
        }
        if query.turn_seq.is_some() && query.session_id.is_none() {
            return Err(RepoError::invalid_argument(
                "turn-scoped search requires session_id",
            ));
        }
        let requested_n_hits = query.n_hits.unwrap_or(self.config().max_results).max(1);
        let effective_n_hits = requested_n_hits.min(self.config().max_results);
        let min_should_match = usize::from(
            query
                .min_should_match
                .unwrap_or(self.config().bm25_default_min_should_match)
                .max(1)
                .min(terms.len() as u16),
        );
        let minimum_score = query
            .min_score
            .unwrap_or(self.config().bm25_default_min_score);
        let all = self.stored_events(None).await?;
        let visible_sessions = group_sessions(all.clone())
            .into_iter()
            .filter(|(_, events)| session_visible(events, self.config()))
            .map(|(session_id, _)| session_id)
            .collect::<HashSet<_>>();
        let scope_exists = query.session_id.as_ref().is_none_or(|session_id| {
            visible_sessions.contains(session_id)
                && all.iter().any(|event| {
                    event.trace.session_id == *session_id
                        && query
                            .turn_seq
                            .is_none_or(|turn_seq| event.trace.turn_seq == turn_seq)
                })
        });
        let (docs, average_length, document_frequencies) = corpus_stats(&all, &terms);
        let k1 = self.config().bm25_k1.max(0.01);
        let b = self.config().bm25_b.clamp(0.0, 1.0);
        let mut scored = all
            .iter()
            .filter(|event| {
                visible_sessions.contains(&event.trace.session_id)
                    && query
                        .session_id
                        .as_deref()
                        .is_none_or(|session| event.trace.session_id == session)
                    && query
                        .turn_seq
                        .is_none_or(|turn| event.trace.turn_seq == turn)
                    && query
                        .harness
                        .as_deref()
                        .is_none_or(|harness| event.harness == harness)
                    && query
                        .source_name
                        .as_deref()
                        .is_none_or(|source| event.source_name == source)
                    && event.source_name != "codex-mcp"
                    && event_types.contains(&McpEventType::from_normalized(event.event_type()))
            })
            .filter_map(|event| {
                let counts = token_counts(&searchable_text(event));
                let (score, matched_terms, doc_len) = bm25_score(
                    &counts,
                    &terms,
                    docs,
                    average_length,
                    &document_frequencies,
                    k1,
                    b,
                );
                if matched_terms < min_should_match as u64 || score < minimum_score {
                    return None;
                }
                Some((event.clone(), score, matched_terms, doc_len))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|(left, left_score, _, _), (right, right_score, _, _)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| right.trace.event_unix_ms.cmp(&left.trace.event_unix_ms))
                .then_with(|| left.trace.event_uid.cmp(&right.trace.event_uid))
        });
        scored.truncate(usize::from(effective_n_hits));
        let hits = scored
            .into_iter()
            .enumerate()
            .filter_map(|(index, (event, score, matched_terms, doc_len))| {
                let session_events = all
                    .iter()
                    .filter(|candidate| candidate.trace.session_id == event.trace.session_id)
                    .cloned()
                    .collect::<Vec<_>>();
                let summary = conversation_summary(&session_events)?;
                let turns = group_turns(&session_events);
                let turn_events = turns.get(&event.trace.turn_seq)?;
                let event_ordinal = turn_events
                    .iter()
                    .position(|candidate| candidate.trace.event_uid == event.trace.event_uid)?
                    as u32
                    + 1;
                let turn_ordinal = turns
                    .keys()
                    .position(|turn_seq| *turn_seq == event.trace.turn_seq)?
                    as u32
                    + 1;
                let (turn_completed, turn_terminal_event_uid) = terminal(turn_events);
                let snippet: String = string(&event.row, "text_preview")
                    .chars()
                    .take(usize::from(self.config().preview_chars))
                    .collect();
                let event_type = McpEventType::from_normalized(event.event_type());
                Some(SearchMcpEventHit {
                    rank: index + 1,
                    event_uid: event.trace.event_uid,
                    source_host: string(&event.row, "source_host"),
                    session_id: event.trace.session_id,
                    event_type,
                    event_time: event.trace.event_time,
                    event_unix_ms: event.trace.event_unix_ms,
                    turn_seq: event.trace.turn_seq,
                    turn_ordinal,
                    event_order: event.trace.event_order,
                    event_ordinal,
                    turn_event_count: turn_events.len() as u64,
                    turn_completed,
                    turn_terminal_event_uid,
                    session_started_at_unix_ms: Some(summary.first_event_unix_ms),
                    session_updated_at_unix_ms: Some(summary.last_event_unix_ms),
                    session_title: summary.session_summary.clone(),
                    session_slug: summary.session_slug,
                    session_summary: summary.session_summary,
                    session_completed: terminal(&session_events).0,
                    source_name: non_empty(event.source_name),
                    harness: non_empty(event.harness),
                    inference_provider: non_empty(event.inference_provider),
                    event_class: event.trace.event_class,
                    payload_type: event.trace.payload_type,
                    actor_role: event.trace.actor_role,
                    tool_name: non_empty(event.trace.name),
                    tool_phase: non_empty(event.trace.phase),
                    call_id: non_empty(event.trace.call_id),
                    item_id: non_empty(event.trace.item_id),
                    model: non_empty(event.model),
                    endpoint_kind: non_empty(event.trace.endpoint_kind),
                    source_ref: non_empty(event.trace.source_ref),
                    snippet_truncated: snippet.chars().count()
                        < string(&event.row, "text_preview").chars().count(),
                    snippet,
                    text_content: non_empty(event.trace.text_content),
                    payload_json: non_empty(event.trace.payload_json),
                    score,
                    raw_score: score,
                    matched_terms,
                    doc_len,
                })
            })
            .collect::<Vec<_>>();
        Ok(SearchMcpEventsResult {
            query_id: query_id(&query.query),
            query: query.query,
            terms,
            event_types,
            scope_exists,
            truncated: false,
            stats: SearchMcpEventsStats {
                docs,
                avgdl: average_length,
                took_ms: 0,
                result_count: hits.len(),
                requested_n_hits,
                effective_n_hits,
                limit_capped: requested_n_hits > effective_n_hits,
                truncated: false,
            },
            hits,
        })
    }

    async fn search_conversations(
        &self,
        query: ConversationSearchQuery,
    ) -> RepoResult<ConversationSearchResults> {
        let terms = search_terms(&query.query, self.config().bm25_max_query_terms);
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "query must contain at least one searchable term",
            ));
        }
        let requested_limit = query.limit.unwrap_or(self.config().max_results).max(1);
        let effective_limit = requested_limit.min(self.config().max_results);
        let min_should_match = usize::from(query.min_should_match.unwrap_or(1).max(1));
        let sessions = group_sessions(self.stored_events(None).await?);
        let docs = sessions.len() as u64;
        let mut scored = sessions
            .into_values()
            .filter_map(|events| {
                let summary = conversation_summary(&events)?;
                if query
                    .from_unix_ms
                    .is_some_and(|from| summary.last_event_unix_ms < from)
                    || query
                        .to_unix_ms
                        .is_some_and(|to| summary.last_event_unix_ms >= to)
                    || query.mode.is_some_and(|mode| summary.mode != mode)
                {
                    return None;
                }
                let best = events
                    .iter()
                    .filter(|event| {
                        query.include_tool_events.unwrap_or(true)
                            || !matches!(
                                event.trace.event_class.as_str(),
                                "tool_call" | "tool_result"
                            )
                    })
                    .filter_map(|event| {
                        let text = searchable_text(event);
                        let matched = terms.iter().filter(|term| text.contains(*term)).count();
                        (matched >= min_should_match).then_some((event, matched, text.len()))
                    })
                    .max_by_key(|(_, matched, _)| *matched)?;
                let score = best.1 as f64 / best.2.max(1) as f64;
                if query.min_score.is_some_and(|minimum| score < minimum) {
                    return None;
                }
                Some((summary, best.0.clone(), score, best.1 as u16, events.len()))
            })
            .collect::<Vec<_>>();
        scored.sort_by(
            |(left, _, left_score, _, _), (right, _, right_score, _, _)| {
                right_score
                    .total_cmp(left_score)
                    .then_with(|| right.last_event_unix_ms.cmp(&left.last_event_unix_ms))
                    .then_with(|| left.session_id.cmp(&right.session_id))
            },
        );
        scored.truncate(usize::from(effective_limit));
        let hits = scored
            .into_iter()
            .enumerate()
            .map(
                |(index, (summary, event, score, matched_terms, event_count))| {
                    ConversationSearchHit {
                        rank: index + 1,
                        session_id: summary.session_id,
                        first_event_time: non_empty(summary.first_event_time),
                        first_event_unix_ms: Some(summary.first_event_unix_ms),
                        last_event_time: non_empty(summary.last_event_time),
                        last_event_unix_ms: Some(summary.last_event_unix_ms),
                        harness: non_empty(event.harness),
                        inference_provider: non_empty(event.inference_provider),
                        session_slug: summary.session_slug,
                        session_summary: summary.session_summary,
                        score,
                        matched_terms,
                        event_count_considered: event_count as u32,
                        best_event_uid: Some(event.trace.event_uid),
                        snippet: non_empty(string(&event.row, "text_preview")),
                        text_preview: non_empty(string(&event.row, "text_preview")),
                        text_content: non_empty(event.trace.text_content),
                        payload_json: non_empty(event.trace.payload_json),
                    }
                },
            )
            .collect::<Vec<_>>();
        Ok(ConversationSearchResults {
            query_id: query_id(&query.query),
            query: query.query,
            terms,
            stats: ConversationSearchStats {
                docs,
                avgdl: 0.0,
                took_ms: 0,
                result_count: hits.len(),
                requested_limit,
                effective_limit,
                limit_capped: requested_limit > effective_limit,
            },
            hits,
        })
    }

    async fn search_session_metadata(
        &self,
        query: SessionMetadataSearchQuery,
    ) -> RepoResult<SessionMetadataSearchResults> {
        let terms = search_terms(&query.query, self.config().bm25_max_query_terms);
        if terms.is_empty() {
            return Err(RepoError::invalid_argument(
                "query must contain at least one searchable term",
            ));
        }
        let requested_limit = query.limit.unwrap_or(self.config().max_results).max(1);
        let effective_limit = requested_limit.min(self.config().max_results);
        let min_should_match = usize::from(query.min_should_match.unwrap_or(1).max(1));
        let mut hits = group_sessions(self.stored_events(None).await?)
            .into_values()
            .filter_map(|events| {
                let summary = conversation_summary(&events)?;
                if query
                    .session_id
                    .as_deref()
                    .is_some_and(|session| summary.session_id != session)
                    || query
                        .from_unix_ms
                        .is_some_and(|from| summary.last_event_unix_ms < from)
                    || query
                        .to_unix_ms
                        .is_some_and(|to| summary.last_event_unix_ms >= to)
                    || query.mode.is_some_and(|mode| summary.mode != mode)
                {
                    return None;
                }
                let meta = events
                    .iter()
                    .rev()
                    .find(|event| event.trace.event_class == "session_meta")?;
                let haystack = format!(
                    "{} {} {} {} {}",
                    summary.session_id,
                    summary.session_slug.as_deref().unwrap_or_default(),
                    summary.session_summary.as_deref().unwrap_or_default(),
                    meta.harness,
                    meta.inference_provider
                )
                .to_ascii_lowercase();
                let matched = terms.iter().filter(|term| haystack.contains(*term)).count();
                if matched < min_should_match {
                    return None;
                }
                let score = matched as f64 / terms.len() as f64;
                if query.min_score.is_some_and(|minimum| score < minimum) {
                    return None;
                }
                Some((summary, meta.clone(), score, matched as u16))
            })
            .collect::<Vec<_>>();
        hits.sort_by(|(left, _, left_score, _), (right, _, right_score, _)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| right.last_event_unix_ms.cmp(&left.last_event_unix_ms))
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        hits.truncate(usize::from(effective_limit));
        let hits = hits
            .into_iter()
            .enumerate()
            .map(
                |(index, (summary, meta, score, matched_terms))| SessionMetadataSearchHit {
                    rank: index + 1,
                    session_id: summary.session_id,
                    first_event_time: non_empty(summary.first_event_time),
                    first_event_unix_ms: Some(summary.first_event_unix_ms),
                    last_event_time: non_empty(summary.last_event_time),
                    last_event_unix_ms: Some(summary.last_event_unix_ms),
                    total_turns: Some(summary.total_turns),
                    total_events: Some(summary.total_events),
                    user_messages: Some(summary.user_messages),
                    assistant_messages: Some(summary.assistant_messages),
                    tool_calls: Some(summary.tool_calls),
                    tool_results: Some(summary.tool_results),
                    mode: Some(summary.mode),
                    harness: non_empty(meta.harness),
                    inference_provider: non_empty(meta.inference_provider),
                    session_slug: summary.session_slug,
                    session_summary: summary.session_summary.clone(),
                    meta_event_uid: Some(meta.trace.event_uid),
                    score,
                    matched_terms,
                    snippet: summary.session_summary,
                },
            )
            .collect::<Vec<_>>();
        Ok(SessionMetadataSearchResults {
            query_id: query_id(&query.query),
            query: query.query,
            terms,
            stats: SessionMetadataSearchStats {
                requested_limit,
                effective_limit,
                limit_capped: requested_limit > effective_limit,
                result_count: hits.len(),
                took_ms: 0,
            },
            hits,
        })
    }

    async fn file_attention(
        &self,
        query: FileAttentionQuery,
    ) -> RepoResult<Vec<FileAttentionTouch>> {
        if query.rel.trim().is_empty() || query.rel.starts_with('/') || query.rel.contains("..") {
            return Err(RepoError::invalid_argument(
                "file attention path must be a project-relative path",
            ));
        }
        let all = self.stored_events(None).await?;
        let mut touches = Vec::new();
        for event in &all {
            if event.trace.event_class != "tool_call"
                || query
                    .start_unix_ms
                    .is_some_and(|start| event.trace.event_unix_ms < start)
                || query
                    .end_unix_ms
                    .is_some_and(|end| event.trace.event_unix_ms >= end)
                || query
                    .harness
                    .as_deref()
                    .is_some_and(|harness| event.harness != harness)
                || query
                    .source_name
                    .as_deref()
                    .is_some_and(|source| event.source_name != source)
                || query
                    .tool
                    .as_deref()
                    .is_some_and(|tool| !event.trace.name.eq_ignore_ascii_case(tool))
                || (query.mutations_only
                    && matches!(
                        event.trace.name.to_ascii_lowercase().as_str(),
                        "read" | "read_file" | "cat" | "grep" | "glob" | "search"
                    ))
            {
                continue;
            }
            if query.apply_project_scope {
                let project_id = string(&event.row, "project_id");
                let cwd = string(&event.row, "cwd");
                let project_matches = query
                    .normalized_project_id
                    .as_deref()
                    .is_some_and(|expected| project_id == expected)
                    || query
                        .normalized_project_roots
                        .iter()
                        .any(|root| cwd == *root || cwd.starts_with(&format!("{root}/")));
                if !project_matches {
                    continue;
                }
            }
            let payload = serde_json::from_str::<serde_json::Value>(&event.trace.payload_json)
                .unwrap_or(serde_json::Value::Null);
            let Some((match_kind, matched_path)) = find_path_match(&payload, &query.rel) else {
                continue;
            };
            let output_preview = all
                .iter()
                .find(|candidate| {
                    candidate.trace.event_class == "tool_result"
                        && !event.trace.call_id.is_empty()
                        && candidate.trace.call_id == event.trace.call_id
                })
                .map(|candidate| string(&candidate.row, "text_preview"))
                .unwrap_or_default();
            touches.push(FileAttentionTouch {
                session_id: event.trace.session_id.clone(),
                event_uid: event.trace.event_uid.clone(),
                tool_call_id: event.trace.call_id.clone(),
                harness: event.harness.clone(),
                source_name: event.source_name.clone(),
                tool_name: event.trace.name.clone(),
                tool_phase: event.trace.phase.clone(),
                match_kind,
                worktree_root: worktree_root(&matched_path, &query.rel),
                matched_path,
                cwd: string(&event.row, "cwd"),
                event_unix_ms: Some(event.trace.event_unix_ms),
                event_order: event.trace.event_order,
                turn_seq: Some(event.trace.turn_seq),
                input_preview: string(&event.row, "text_preview"),
                output_preview,
            });
            if touches.len() >= query.max_rows {
                break;
            }
        }
        touches.sort_by(|left, right| {
            right
                .event_unix_ms
                .cmp(&left.event_unix_ms)
                .then_with(|| right.event_order.cmp(&left.event_order))
                .then_with(|| left.event_uid.cmp(&right.event_uid))
        });
        Ok(touches)
    }
}
