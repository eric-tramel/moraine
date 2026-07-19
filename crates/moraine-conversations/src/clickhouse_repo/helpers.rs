use super::*;

impl ClickHouseConversationRepository {
    pub(super) fn mode_subquery(&self) -> String {
        self.mode_subquery_for_sessions(None)
    }

    pub(super) fn mode_aggregate_sql() -> String {
        let mcp_name_predicate = moraine_clickhouse::mcp_tool_names::sql_predicate("tool_name");
        format!(
            "multiIf(
    countIf(
      payload_type = 'web_search_call'
      OR payload_type = 'search_results_received'
      OR (payload_type = 'tool_use' AND tool_name IN ('WebSearch', 'WebFetch'))
    ) > 0,
    'web_search',
    countIf(source_name = 'codex-mcp' OR {mcp_name_predicate}) > 0,
    'mcp_internal',
    countIf(event_kind IN ('tool_call', 'tool_result') OR payload_type = 'tool_use') > 0,
    'tool_calling',
    'chat'
  )"
        )
    }

    pub(super) fn mode_subquery_for_sessions(&self, session_ids_sql: Option<&str>) -> String {
        let events_source = canonical_events_source(&self.table_ref("events"));
        let mode_aggregate = Self::mode_aggregate_sql();
        let session_filter = session_ids_sql
            .map(|session_ids_sql| format!("WHERE session_id IN ({session_ids_sql})\n"))
            .unwrap_or_default();
        format!(
            "SELECT
  session_id,
  {mode_aggregate} AS mode
FROM {events_source}
{session_filter}GROUP BY session_id"
        )
    }

    pub(super) fn is_mcp_internal_tool_name(name: &str) -> bool {
        moraine_clickhouse::mcp_tool_names::is_internal_tool_name(name)
    }

    pub(super) fn parse_mode(raw: &str) -> ConversationMode {
        match raw {
            "web_search" => ConversationMode::WebSearch,
            "mcp_internal" => ConversationMode::McpInternal,
            "tool_calling" => ConversationMode::ToolCalling,
            _ => ConversationMode::Chat,
        }
    }

    pub(super) fn conversation_filter_sig(filter: &ConversationListFilter) -> String {
        format!(
            "from={:?};to={:?};mode={};sort={}",
            filter.from_unix_ms,
            filter.to_unix_ms,
            filter
                .mode
                .map(ConversationMode::as_str)
                .unwrap_or("__none__"),
            filter.sort.as_str(),
        )
    }

    pub(super) fn mcp_session_list_filter_sig(&self, filter: &McpSessionListFilter) -> String {
        // The session scope participates in the signature so a cursor minted
        // by an unscoped (or differently scoped) server is rejected instead
        // of silently resuming with different visibility.
        serde_json::to_string(&(
            filter.start_unix_ms,
            filter.end_unix_ms,
            filter.mode.map(ConversationMode::as_str),
            filter.harness.as_deref(),
            filter.source_name.as_deref(),
            filter.sort.as_str(),
            self.cfg
                .session_scope
                .as_ref()
                .map(|scope| scope.roots.as_slice()),
        ))
        .expect("list_sessions cursor filter contains only serializable primitives")
    }

    pub(super) fn turn_filter_sig(session_id: &str, filter: &TurnListFilter) -> String {
        format!(
            "session={};from={:?};to={:?}",
            session_id, filter.from_turn_seq, filter.to_turn_seq
        )
    }

    pub(super) fn session_events_filter_sig(
        session_id: &str,
        direction: SessionEventsDirection,
        event_kinds: Option<&[SearchEventKind]>,
    ) -> String {
        let event_kind_sig = event_kinds
            .map(|kinds| {
                kinds
                    .iter()
                    .map(|kind| kind.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_else(|| "__none__".to_string());
        format!(
            "session={session_id};direction={};event_kinds={event_kind_sig}",
            direction.as_str()
        )
    }

    pub(super) fn validate_time_bounds(
        from_unix_ms: Option<i64>,
        to_unix_ms: Option<i64>,
    ) -> RepoResult<()> {
        if let (Some(from), Some(to)) = (from_unix_ms, to_unix_ms) {
            if from >= to {
                return Err(RepoError::invalid_argument(
                    "from_unix_ms must be strictly less than to_unix_ms",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_required_time_bounds(
        start_unix_ms: i64,
        end_unix_ms: i64,
    ) -> RepoResult<()> {
        if start_unix_ms >= end_unix_ms {
            return Err(RepoError::invalid_argument(
                "start_unix_ms must be strictly less than end_unix_ms",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_session_id(session_id: &str) -> RepoResult<()> {
        if !is_safe_filter_value(session_id) {
            return Err(RepoError::invalid_argument(
                "session_id contains unsupported characters",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_event_uid(event_uid: &str) -> RepoResult<()> {
        if !is_safe_filter_value(event_uid) {
            return Err(RepoError::invalid_argument(
                "event_uid contains unsupported characters",
            ));
        }
        Ok(())
    }

    pub(super) fn normalize_event_kinds(
        event_kinds: Option<Vec<SearchEventKind>>,
    ) -> RepoResult<Option<Vec<SearchEventKind>>> {
        let Some(mut kinds) = event_kinds else {
            return Ok(None);
        };

        if kinds.is_empty() {
            return Err(RepoError::invalid_argument(
                "event_kind filter cannot be an empty list",
            ));
        }

        kinds.sort_unstable();
        kinds.dedup();
        Ok(Some(kinds))
    }

    pub(super) fn normalize_mcp_event_types(
        event_types: Option<Vec<McpEventType>>,
    ) -> RepoResult<Vec<McpEventType>> {
        let mut event_types = event_types.unwrap_or_else(McpEventType::default_search_types);
        if event_types.is_empty() {
            return Err(RepoError::invalid_argument(
                "event_types filter cannot be an empty list",
            ));
        }

        event_types.sort_by_key(|event_type| event_type.search_order());
        event_types.dedup();
        Ok(event_types)
    }

    pub(super) fn is_mcp_system_event(
        event_class: &str,
        payload_type: &str,
        actor_role: &str,
    ) -> bool {
        actor_role.eq_ignore_ascii_case("system")
            || matches!(event_class, "system" | "progress" | "file_history_snapshot")
            || matches!(
                payload_type,
                "system" | "progress" | "file-history-snapshot" | "file_history_snapshot"
            )
    }

    pub(super) fn is_mcp_runtime_event(event_class: &str, payload_type: &str) -> bool {
        event_class == "queue_operation"
            || matches!(
                payload_type,
                "task_started"
                    | "task_complete"
                    | "turn_aborted"
                    | "item_completed"
                    | "queue-operation"
            )
    }

    pub(super) fn is_mcp_message_event(event_class: &str, payload_type: &str) -> bool {
        event_class == "message"
            || (event_class == "event_msg"
                && matches!(
                    payload_type,
                    "user_message" | "agent_message" | "message" | "text" | "event_msg"
                ))
    }

    pub(super) fn is_mcp_reasoning_event(event_class: &str, payload_type: &str) -> bool {
        event_class == "reasoning"
            || matches!(payload_type, "agent_reasoning" | "reasoning" | "thinking")
    }

    pub(super) fn is_mcp_tool_call_event(event_class: &str, payload_type: &str) -> bool {
        event_class == "tool_call"
            || matches!(
                payload_type,
                "tool_use" | "function_call" | "custom_tool_call" | "web_search_call"
            )
    }

    pub(super) fn is_mcp_tool_response_event(event_class: &str, payload_type: &str) -> bool {
        event_class == "tool_result"
            || matches!(
                payload_type,
                "tool_result"
                    | "function_call_output"
                    | "custom_tool_call_output"
                    | "search_results_received"
            )
    }

    pub(super) fn is_mcp_compaction_event(event_class: &str, payload_type: &str) -> bool {
        matches!(event_class, "compacted_raw" | "summary")
            || matches!(payload_type, "compacted" | "summary")
    }

    pub(super) fn mcp_event_type_for(
        event_class: &str,
        payload_type: &str,
        actor_role: &str,
    ) -> McpEventType {
        let is_system_actor = actor_role.eq_ignore_ascii_case("system");
        if actor_role.eq_ignore_ascii_case("user")
            && Self::is_mcp_message_event(event_class, payload_type)
        {
            McpEventType::UserInput
        } else if actor_role.eq_ignore_ascii_case("assistant")
            && Self::is_mcp_message_event(event_class, payload_type)
        {
            McpEventType::AssistantResponse
        } else if !is_system_actor && Self::is_mcp_reasoning_event(event_class, payload_type) {
            McpEventType::Reasoning
        } else if !is_system_actor && Self::is_mcp_tool_call_event(event_class, payload_type) {
            McpEventType::ToolCall
        } else if !is_system_actor && Self::is_mcp_tool_response_event(event_class, payload_type) {
            McpEventType::ToolResponse
        } else if Self::is_mcp_compaction_event(event_class, payload_type) {
            McpEventType::Compaction
        } else if Self::is_mcp_runtime_event(event_class, payload_type) {
            McpEventType::Runtime
        } else if Self::is_mcp_system_event(event_class, payload_type, actor_role) {
            McpEventType::System
        } else {
            McpEventType::Unknown
        }
    }

    pub(super) fn mcp_message_event_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
    ) -> String {
        format!(
            "({event_class_expr} = 'message' OR ({event_class_expr} = 'event_msg' AND {payload_type_expr} IN ('user_message', 'agent_message', 'message', 'text', 'event_msg')))"
        )
    }

    pub(super) fn mcp_reasoning_event_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
    ) -> String {
        format!(
            "({event_class_expr} = 'reasoning' OR {payload_type_expr} IN ('agent_reasoning', 'reasoning', 'thinking'))"
        )
    }

    pub(super) fn mcp_tool_call_event_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
    ) -> String {
        format!(
            "({event_class_expr} = 'tool_call' OR {payload_type_expr} IN ('tool_use', 'function_call', 'custom_tool_call', 'web_search_call'))"
        )
    }

    pub(super) fn mcp_tool_response_event_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
    ) -> String {
        format!(
            "({event_class_expr} = 'tool_result' OR {payload_type_expr} IN ('tool_result', 'function_call_output', 'custom_tool_call_output', 'search_results_received'))"
        )
    }

    pub(super) fn mcp_compaction_event_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
    ) -> String {
        format!(
            "({event_class_expr} IN ('compacted_raw', 'summary') OR {payload_type_expr} IN ('compacted', 'summary'))"
        )
    }

    pub(super) fn mcp_runtime_event_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
    ) -> String {
        format!(
            "({event_class_expr} = 'queue_operation' OR {payload_type_expr} IN ('task_started', 'task_complete', 'turn_aborted', 'item_completed', 'queue-operation'))"
        )
    }

    pub(super) fn mcp_system_event_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
        actor_role_expr: &str,
    ) -> String {
        format!(
            "(lowerUTF8({actor_role_expr}) = 'system' OR {event_class_expr} IN ('system', 'progress', 'file_history_snapshot') OR {payload_type_expr} IN ('system', 'progress', 'file-history-snapshot', 'file_history_snapshot'))"
        )
    }

    pub(super) fn single_mcp_event_type_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
        actor_role_expr: &str,
        event_type: McpEventType,
    ) -> String {
        let non_system_actor = format!("lowerUTF8({actor_role_expr}) != 'system'");
        match event_type {
            McpEventType::UserInput => format!(
                "(lowerUTF8({actor_role_expr}) = 'user' AND {})",
                Self::mcp_message_event_clause(event_class_expr, payload_type_expr)
            ),
            McpEventType::AssistantResponse => format!(
                "(lowerUTF8({actor_role_expr}) = 'assistant' AND {})",
                Self::mcp_message_event_clause(event_class_expr, payload_type_expr)
            ),
            McpEventType::Reasoning => format!(
                "({non_system_actor} AND {})",
                Self::mcp_reasoning_event_clause(event_class_expr, payload_type_expr)
            ),
            McpEventType::ToolCall => format!(
                "({non_system_actor} AND {})",
                Self::mcp_tool_call_event_clause(event_class_expr, payload_type_expr)
            ),
            McpEventType::ToolResponse => format!(
                "({non_system_actor} AND {})",
                Self::mcp_tool_response_event_clause(event_class_expr, payload_type_expr)
            ),
            McpEventType::Compaction => {
                Self::mcp_compaction_event_clause(event_class_expr, payload_type_expr)
            }
            McpEventType::System => {
                Self::mcp_system_event_clause(event_class_expr, payload_type_expr, actor_role_expr)
            }
            McpEventType::Runtime => {
                Self::mcp_runtime_event_clause(event_class_expr, payload_type_expr)
            }
            McpEventType::Unknown => {
                let known = [
                    McpEventType::UserInput,
                    McpEventType::AssistantResponse,
                    McpEventType::Reasoning,
                    McpEventType::ToolCall,
                    McpEventType::ToolResponse,
                    McpEventType::Compaction,
                    McpEventType::System,
                    McpEventType::Runtime,
                ]
                .into_iter()
                .map(|known_type| {
                    Self::single_mcp_event_type_clause(
                        event_class_expr,
                        payload_type_expr,
                        actor_role_expr,
                        known_type,
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
                format!("NOT ({known})")
            }
        }
    }

    pub(super) fn mcp_event_type_filter_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
        actor_role_expr: &str,
        event_types: &[McpEventType],
    ) -> String {
        let clauses = event_types
            .iter()
            .map(|event_type| {
                Self::single_mcp_event_type_clause(
                    event_class_expr,
                    payload_type_expr,
                    actor_role_expr,
                    *event_type,
                )
            })
            .collect::<Vec<_>>();
        if clauses.len() == 1 {
            clauses[0].clone()
        } else {
            format!("({})", clauses.join(" OR "))
        }
    }

    pub(super) fn matches_event_kind(
        event_class: &str,
        payload_type: &str,
        kind: SearchEventKind,
    ) -> bool {
        match kind {
            SearchEventKind::Message => {
                event_class == "message"
                    || (event_class == "event_msg"
                        && matches!(
                            payload_type,
                            "user_message" | "agent_message" | "message" | "text"
                        ))
            }
            SearchEventKind::Reasoning => {
                event_class == "reasoning"
                    || (event_class == "event_msg" && payload_type == "agent_reasoning")
            }
            SearchEventKind::ToolCall => {
                event_class == "tool_call"
                    || (event_class == "event_msg"
                        && matches!(
                            payload_type,
                            "tool_use" | "function_call" | "custom_tool_call" | "web_search_call"
                        ))
            }
            SearchEventKind::ToolResult => {
                event_class == "tool_result"
                    || (event_class == "event_msg"
                        && matches!(
                            payload_type,
                            "tool_result"
                                | "function_call_output"
                                | "custom_tool_call_output"
                                | "search_results_received"
                        ))
            }
        }
    }

    pub(super) fn matches_requested_event_kinds(
        event_class: &str,
        payload_type: &str,
        event_kinds: &[SearchEventKind],
    ) -> bool {
        event_kinds
            .iter()
            .any(|kind| Self::matches_event_kind(event_class, payload_type, *kind))
    }

    pub(super) fn single_event_kind_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
        kind: SearchEventKind,
    ) -> String {
        match kind {
            SearchEventKind::Message => format!(
                "({event_class_expr} = 'message' OR ({event_class_expr} = 'event_msg' AND {payload_type_expr} IN ('user_message', 'agent_message', 'message', 'text')))"
            ),
            SearchEventKind::Reasoning => format!(
                "({event_class_expr} = 'reasoning' OR ({event_class_expr} = 'event_msg' AND {payload_type_expr} = 'agent_reasoning'))"
            ),
            SearchEventKind::ToolCall => format!(
                "({event_class_expr} = 'tool_call' OR ({event_class_expr} = 'event_msg' AND {payload_type_expr} IN ('tool_use', 'function_call', 'custom_tool_call', 'web_search_call')))"
            ),
            SearchEventKind::ToolResult => format!(
                "({event_class_expr} = 'tool_result' OR ({event_class_expr} = 'event_msg' AND {payload_type_expr} IN ('tool_result', 'function_call_output', 'custom_tool_call_output', 'search_results_received')))"
            ),
        }
    }

    pub(super) fn event_kind_filter_clause(
        event_class_expr: &str,
        payload_type_expr: &str,
        event_kinds: &[SearchEventKind],
    ) -> String {
        let clauses = event_kinds
            .iter()
            .map(|kind| Self::single_event_kind_clause(event_class_expr, payload_type_expr, *kind))
            .collect::<Vec<_>>();
        if clauses.len() == 1 {
            clauses[0].clone()
        } else {
            format!("({})", clauses.join(" OR "))
        }
    }

    pub(super) fn is_low_information_system_event(actor_role: &str, payload_type: &str) -> bool {
        actor_role.eq_ignore_ascii_case("system")
            && matches!(
                payload_type.to_ascii_lowercase().as_str(),
                "progress" | "file_history_snapshot" | "system"
            )
    }

    pub(super) fn open_context_filter_clause(include_system_events: bool) -> &'static str {
        if include_system_events {
            ""
        } else {
            " AND NOT (lowerUTF8(actor_role) = 'system' AND lowerUTF8(payload_type) IN ('progress', 'file_history_snapshot', 'system'))"
        }
    }

    pub(super) fn mode_filter_clause(mode: Option<ConversationMode>) -> Option<String> {
        mode.map(|m| format!("ifNull(m.mode, 'chat') = {}", sql_quote(m.as_str())))
    }
}

pub(super) fn token_re() -> &'static Regex {
    static TOKEN_RE: OnceLock<Regex> = OnceLock::new();
    TOKEN_RE.get_or_init(|| Regex::new(r"[A-Za-z0-9_]+").expect("valid token regex"))
}

pub(super) fn safe_value_re() -> &'static Regex {
    static SAFE_RE: OnceLock<Regex> = OnceLock::new();
    SAFE_RE
        .get_or_init(|| Regex::new(r"^[A-Za-z0-9._:@/-]{1,256}$").expect("valid safe-value regex"))
}

pub(super) fn tokenize_query(text: &str, max_terms: usize) -> Vec<(String, u32)> {
    let mut order = Vec::<String>::new();
    let mut tf = HashMap::<String, u32>::new();

    for mat in token_re().find_iter(text) {
        let token = mat.as_str().to_ascii_lowercase();
        if token.len() < 2 || token.len() > 64 {
            continue;
        }

        if !tf.contains_key(&token) {
            order.push(token.clone());
        }
        let entry = tf.entry(token).or_insert(0);
        *entry += 1;

        if order.len() >= max_terms {
            break;
        }
    }

    order
        .into_iter()
        .map(|token| {
            let count = *tf.get(&token).unwrap_or(&1);
            (token, count)
        })
        .collect()
}

pub(super) fn is_safe_filter_value(value: &str) -> bool {
    safe_value_re().is_match(value)
}

pub(super) fn non_empty_string(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

pub(super) fn compact_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(super) fn compact_text_line(text: &str, max_chars: usize) -> String {
    let compact = compact_whitespace(text);
    if compact.chars().count() <= max_chars {
        return compact;
    }

    let mut trimmed: String = compact.chars().take(max_chars.saturating_sub(3)).collect();
    trimmed.push_str("...");
    trimmed
}

pub(super) fn first_term_match_index(value: &str, terms: &[String]) -> Option<usize> {
    let lower = value.to_ascii_lowercase();
    terms.iter().filter_map(|term| lower.find(term)).min()
}

pub(super) fn snippet_around_match(
    value: &str,
    match_idx: Option<usize>,
    max_chars: usize,
) -> String {
    let compact = compact_whitespace(value);
    if compact.chars().count() <= max_chars {
        return compact;
    }

    let byte_idx = match_idx
        .and_then(|idx| value.get(..idx))
        .map(compact_whitespace)
        .map(|prefix| prefix.chars().count())
        .unwrap_or(0);
    let context_before = max_chars / 3;
    let start = byte_idx.saturating_sub(context_before);
    let mut snippet = compact
        .chars()
        .skip(start)
        .take(max_chars)
        .collect::<String>();
    if start > 0 {
        snippet.insert_str(0, "...");
    }
    if compact.chars().count() > start + max_chars {
        snippet.push_str("...");
    }
    snippet
}

pub(super) fn evidence_snippet(
    session_summary: &str,
    metadata_text: &str,
    terms: &[String],
    preview_chars: u16,
) -> Option<String> {
    let summary_match = first_term_match_index(session_summary, terms);
    let metadata_match = first_term_match_index(metadata_text, terms);
    let (source, match_idx) = if summary_match.is_some() {
        (session_summary, summary_match)
    } else if metadata_match.is_some() {
        (metadata_text, metadata_match)
    } else if !session_summary.is_empty() {
        (session_summary, None)
    } else {
        (metadata_text, None)
    };
    if source.is_empty() {
        return None;
    }

    let snippet = snippet_around_match(source, match_idx, usize::from(preview_chars).max(1));
    (!snippet.is_empty()).then_some(snippet)
}
