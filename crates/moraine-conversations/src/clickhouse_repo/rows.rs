use super::*;

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ConversationSummaryRow {
    pub(super) session_id: String,
    pub(super) first_event_time: String,
    pub(super) first_event_unix_ms: i64,
    pub(super) last_event_time: String,
    pub(super) last_event_unix_ms: i64,
    pub(super) total_turns: u32,
    pub(super) total_events: u64,
    pub(super) user_messages: u64,
    pub(super) assistant_messages: u64,
    pub(super) tool_calls: u64,
    pub(super) tool_results: u64,
    pub(super) mode: String,
    #[serde(default)]
    pub(super) session_slug: String,
    #[serde(default)]
    pub(super) session_summary: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SessionMetadataRow {
    pub(super) session_id: String,
    pub(super) first_event_time: String,
    pub(super) first_event_unix_ms: i64,
    pub(super) last_event_time: String,
    pub(super) last_event_unix_ms: i64,
    pub(super) total_turns: u32,
    pub(super) total_events: u64,
    pub(super) user_messages: u64,
    pub(super) assistant_messages: u64,
    pub(super) tool_calls: u64,
    pub(super) tool_results: u64,
    pub(super) mode: String,
    #[serde(default)]
    pub(super) first_event_uid: String,
    #[serde(default)]
    pub(super) last_event_uid: String,
    #[serde(default)]
    pub(super) last_actor_role: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct McpSessionListRow {
    pub(super) session_id: String,
    pub(super) first_event_time: String,
    pub(super) first_event_unix_ms: i64,
    pub(super) last_event_time: String,
    pub(super) last_event_unix_ms: i64,
    pub(super) total_turns: u32,
    pub(super) total_events: u64,
    pub(super) mode: String,
    pub(super) completed: u8,
    #[serde(default)]
    pub(super) title: String,
    #[serde(default)]
    pub(super) source: String,
    #[serde(default)]
    pub(super) harness: String,
    #[serde(default)]
    pub(super) session_slug: String,
    #[serde(default)]
    pub(super) session_summary: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SessionMetadataSearchRow {
    pub(super) session_id: String,
    #[serde(default)]
    pub(super) first_event_time: String,
    #[serde(default)]
    pub(super) first_event_unix_ms: i64,
    #[serde(default)]
    pub(super) last_event_time: String,
    #[serde(default)]
    pub(super) last_event_unix_ms: i64,
    #[serde(default)]
    pub(super) total_turns: u32,
    #[serde(default)]
    pub(super) total_events: u64,
    #[serde(default)]
    pub(super) user_messages: u64,
    #[serde(default)]
    pub(super) assistant_messages: u64,
    #[serde(default)]
    pub(super) tool_calls: u64,
    #[serde(default)]
    pub(super) tool_results: u64,
    #[serde(default)]
    pub(super) mode: String,
    #[serde(default)]
    pub(super) harness: String,
    #[serde(default)]
    pub(super) inference_provider: String,
    #[serde(default)]
    pub(super) session_slug: String,
    #[serde(default)]
    pub(super) session_summary: String,
    #[serde(default)]
    pub(super) meta_event_uid: String,
    #[serde(default)]
    pub(super) score: f64,
    #[serde(default)]
    pub(super) matched_terms: u16,
    #[serde(default)]
    pub(super) metadata_text: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct TurnSummaryRow {
    pub(super) session_id: String,
    pub(super) turn_seq: u32,
    pub(super) turn_id: String,
    pub(super) started_at: String,
    pub(super) started_at_unix_ms: i64,
    pub(super) ended_at: String,
    pub(super) ended_at_unix_ms: i64,
    pub(super) total_events: u64,
    pub(super) user_messages: u64,
    pub(super) assistant_messages: u64,
    pub(super) tool_calls: u64,
    pub(super) tool_results: u64,
    pub(super) reasoning_items: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct TraceEventRow {
    pub(super) session_id: String,
    pub(super) event_uid: String,
    pub(super) event_order: u64,
    pub(super) turn_seq: u32,
    pub(super) event_time: String,
    pub(super) event_unix_ms: i64,
    pub(super) actor_role: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    #[serde(default)]
    pub(super) call_id: String,
    #[serde(default)]
    pub(super) name: String,
    #[serde(default)]
    pub(super) phase: String,
    #[serde(default)]
    pub(super) item_id: String,
    #[serde(default)]
    pub(super) source_ref: String,
    #[serde(default)]
    pub(super) text_content: String,
    #[serde(default)]
    pub(super) payload_json: String,
    #[serde(default)]
    pub(super) token_usage_json: String,
    #[serde(default)]
    pub(super) endpoint_kind: String,
    #[serde(default)]
    pub(super) token_usage_buckets: BTreeMap<String, u64>,
    #[serde(default)]
    pub(super) token_usage_native_units: BTreeMap<String, f64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct OpenTargetRow {
    pub(super) session_id: String,
    pub(super) event_order: u64,
    pub(super) turn_seq: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct McpOpenSessionRow {
    pub(super) session_id: String,
    pub(super) slot: u8,
    pub(super) generation: u64,
    pub(super) first_event_time: String,
    pub(super) first_event_unix_ms: i64,
    pub(super) last_event_time: String,
    pub(super) last_event_unix_ms: i64,
    pub(super) total_turns: u32,
    pub(super) total_events: u64,
    pub(super) user_messages: u64,
    pub(super) assistant_messages: u64,
    pub(super) tool_calls: u64,
    pub(super) tool_results: u64,
    pub(super) mode: String,
    pub(super) first_event_uid: String,
    pub(super) last_event_uid: String,
    pub(super) last_actor_role: String,
    pub(super) title: String,
    pub(super) source: String,
    pub(super) harness: String,
    pub(super) inference_provider: String,
    pub(super) session_slug: String,
    pub(super) session_summary: String,
    pub(super) completed: u8,
    pub(super) terminal_event_uid: String,
    pub(super) origin_cwd: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct McpOpenTurnRow {
    pub(super) session_id: String,
    pub(super) turn_seq: u32,
    pub(super) turn_id: String,
    pub(super) started_at: String,
    pub(super) started_at_unix_ms: i64,
    pub(super) ended_at: String,
    pub(super) ended_at_unix_ms: i64,
    pub(super) total_events: u64,
    pub(super) user_messages: u64,
    pub(super) assistant_messages: u64,
    pub(super) tool_calls: u64,
    pub(super) tool_results: u64,
    pub(super) reasoning_items: u64,
    pub(super) user_input_summary_source: String,
    pub(super) final_response_summary_source: String,
    pub(super) user_input_summary_is_payload: u8,
    pub(super) final_response_summary_is_payload: u8,
    pub(super) user_input_event_uid: String,
    pub(super) user_input_event_order: u64,
    pub(super) user_input_event_time: String,
    pub(super) user_input_event_type: String,
    pub(super) final_response_event_uid: String,
    pub(super) final_response_event_order: u64,
    pub(super) final_response_event_time: String,
    pub(super) final_response_event_type: String,
    pub(super) tools_called: Vec<String>,
    pub(super) normalized_event_types: Vec<String>,
    pub(super) completed: u8,
    pub(super) terminal_event_uid: String,
    pub(super) first_event_uid: String,
    pub(super) first_event_order: u64,
    pub(super) first_event_time: String,
    pub(super) first_event_type: String,
    pub(super) last_event_uid: String,
    pub(super) last_event_order: u64,
    pub(super) last_event_time: String,
    pub(super) last_event_type: String,
    pub(super) previous_turn_seq: u32,
    pub(super) previous_turn_id: String,
    pub(super) previous_turn_started_at: String,
    pub(super) previous_turn_ended_at: String,
    pub(super) next_turn_seq: u32,
    pub(super) next_turn_id: String,
    pub(super) next_turn_started_at: String,
    pub(super) next_turn_ended_at: String,
    pub(super) event_summaries_json: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct McpOpenEventLookupRow {
    pub(super) event_uid: String,
    pub(super) session_id: String,
    pub(super) slot: u8,
    pub(super) generation: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct McpOpenEventRow {
    pub(super) session_id: String,
    pub(super) event_uid: String,
    pub(super) event_order: u64,
    pub(super) turn_seq: u32,
    pub(super) event_time: String,
    pub(super) event_unix_ms: i64,
    pub(super) actor_role: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) event_type: String,
    pub(super) event_ordinal: u32,
    pub(super) call_id: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) item_id: String,
    pub(super) source_ref: String,
    pub(super) text_content: String,
    pub(super) payload_json: String,
    pub(super) token_usage_json: String,
    pub(super) endpoint_kind: String,
    pub(super) token_usage_buckets: BTreeMap<String, u64>,
    pub(super) token_usage_native_units: BTreeMap<String, f64>,
    pub(super) previous_event_uid: String,
    pub(super) next_event_uid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ProjectedEventSummaryRow {
    pub(super) event_uid: String,
    pub(super) event_order: u64,
    pub(super) event_time: String,
    pub(super) event_unix_ms: i64,
    pub(super) actor_role: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) event_type: String,
    pub(super) call_id: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) summary_source: String,
    pub(super) summary_is_payload: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchRow {
    pub(super) event_uid: String,
    pub(super) session_id: String,
    #[serde(default)]
    pub(super) event_time: String,
    pub(super) source_name: String,
    pub(super) harness: String,
    #[serde(default)]
    pub(super) inference_provider: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) actor_role: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) source_ref: String,
    pub(super) doc_len: u32,
    pub(super) text_preview: String,
    #[serde(default)]
    pub(super) text_content: String,
    #[serde(default)]
    pub(super) payload_json: String,
    pub(super) score: f64,
    pub(super) matched_terms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchMcpCandidateRow {
    pub(super) row_kind: u8,
    #[serde(default)]
    pub(super) event_uid: String,
    #[serde(default)]
    pub(super) session_id: String,
    #[serde(default)]
    pub(super) slot: u8,
    #[serde(default)]
    pub(super) generation: u64,
    #[serde(default)]
    pub(super) raw_score: f64,
    #[serde(default)]
    pub(super) matched_terms: u64,
    #[serde(default)]
    pub(super) event_unix_ms: i64,
    pub(super) docs: u64,
    pub(super) total_doc_len: u64,
    pub(super) scope_exists: u8,
    pub(super) projection_ready: u8,
    pub(super) projection_clean: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchMcpEventRow {
    pub(super) event_uid: String,
    pub(super) session_id: String,
    pub(super) source_name: String,
    pub(super) harness: String,
    #[serde(default)]
    pub(super) inference_provider: String,
    #[serde(default)]
    pub(super) endpoint_kind: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) actor_role: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) source_ref: String,
    pub(super) doc_len: u32,
    pub(super) text_preview: String,
    #[serde(default)]
    pub(super) text_content: String,
    #[serde(default)]
    pub(super) payload_json: String,
    #[serde(default)]
    pub(super) mcp_event_type: String,
    pub(super) raw_score: f64,
    pub(super) matched_terms: u64,
    pub(super) event_time: String,
    pub(super) event_unix_ms: i64,
    pub(super) event_order: u64,
    pub(super) turn_seq: u32,
    #[serde(default)]
    pub(super) event_ordinal: u32,
    #[serde(default)]
    pub(super) turn_event_count: u64,
    #[serde(default)]
    pub(super) turn_completed: u8,
    #[serde(default)]
    pub(super) turn_terminal_event_uid: String,
    #[serde(default)]
    pub(super) call_id: String,
    #[serde(default)]
    pub(super) item_id: String,
    #[serde(default)]
    pub(super) model: String,
    #[serde(default)]
    pub(super) session_started_at_unix_ms: i64,
    #[serde(default)]
    pub(super) session_updated_at_unix_ms: i64,
    #[serde(default)]
    pub(super) session_title: String,
    #[serde(default)]
    pub(super) session_slug: String,
    #[serde(default)]
    pub(super) session_summary: String,
    #[serde(default)]
    pub(super) session_completed: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct CachedPostingRow {
    pub(super) event_uid: String,
    pub(super) doc_len: u32,
    pub(super) tf: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct FetchedPostingRow {
    pub(super) term: String,
    pub(super) event_uid: String,
    pub(super) doc_len: u32,
    pub(super) tf: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SearchDocExtraRow {
    pub(super) event_uid: String,
    pub(super) session_id: String,
    #[serde(default)]
    pub(super) event_time: String,
    pub(super) source_name: String,
    pub(super) harness: String,
    #[serde(default)]
    pub(super) inference_provider: String,
    pub(super) event_class: String,
    pub(super) payload_type: String,
    pub(super) actor_role: String,
    pub(super) name: String,
    pub(super) phase: String,
    pub(super) source_ref: String,
    pub(super) doc_len: u32,
    pub(super) text_preview: String,
    #[serde(default)]
    pub(super) text_content: String,
    #[serde(default)]
    pub(super) payload_json: String,
    pub(super) has_codex_mcp: u8,
}

#[derive(Debug, Deserialize)]
pub(super) struct CorpusStatsRow {
    pub(super) docs: u64,
    pub(super) total_doc_len: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct DfRow {
    pub(super) term: String,
    pub(super) df: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct HotQueryRow {
    pub(super) raw_query: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConversationSearchRow {
    pub(super) session_id: String,
    #[serde(default)]
    pub(super) first_event_time: String,
    #[serde(default)]
    pub(super) first_event_unix_ms: i64,
    #[serde(default)]
    pub(super) last_event_time: String,
    #[serde(default)]
    pub(super) last_event_unix_ms: i64,
    #[serde(default)]
    pub(super) harness: String,
    #[serde(default)]
    pub(super) inference_provider: String,
    pub(super) score: f64,
    pub(super) matched_terms: u16,
    pub(super) event_count_considered: u32,
    pub(super) best_event_uid: String,
    #[serde(default)]
    pub(super) snippet: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConversationSessionMetadataRow {
    pub(super) session_id: String,
    #[serde(default)]
    pub(super) harness: String,
    #[serde(default)]
    pub(super) inference_provider: String,
    #[serde(default)]
    pub(super) session_slug: String,
    #[serde(default)]
    pub(super) session_summary: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConversationSnippetRow {
    pub(super) event_uid: String,
    pub(super) snippet: String,
    #[serde(default)]
    pub(super) text_content: String,
    #[serde(default)]
    pub(super) payload_json: String,
    #[serde(default)]
    pub(super) event_class: String,
    #[serde(default)]
    pub(super) actor_role: String,
}

#[derive(Debug, Clone)]
pub(super) struct ConversationSnippetContent {
    pub(super) snippet: String,
    pub(super) text_content: Option<String>,
    pub(super) payload_json: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SessionTimeBoundsRow {
    pub(super) session_id: String,
    pub(super) first_event_time: String,
    pub(super) last_event_time: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConversationCandidateRow {
    pub(super) session_id: String,
    pub(super) score: f64,
    pub(super) matched_terms: u16,
}

#[derive(Debug, Default)]
pub(super) struct ConversationCandidateSet {
    pub(super) rows: Vec<ConversationCandidateRow>,
    pub(super) truncated: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct SessionTimeBounds {
    pub(super) first_event_time: String,
    pub(super) last_event_time: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ColumnExistsRow {
    pub(super) exists: u8,
}

impl ClickHouseConversationRepository {
    pub(super) fn map_conversation_row(row: ConversationSummaryRow) -> ConversationSummary {
        ConversationSummary {
            session_id: row.session_id,
            first_event_time: row.first_event_time,
            first_event_unix_ms: row.first_event_unix_ms,
            last_event_time: row.last_event_time,
            last_event_unix_ms: row.last_event_unix_ms,
            total_turns: row.total_turns,
            total_events: row.total_events,
            user_messages: row.user_messages,
            assistant_messages: row.assistant_messages,
            tool_calls: row.tool_calls,
            tool_results: row.tool_results,
            mode: Self::parse_mode(&row.mode),
            session_slug: non_empty_string(row.session_slug),
            session_summary: non_empty_string(row.session_summary),
        }
    }

    pub(super) fn map_mcp_session_list_row(row: McpSessionListRow) -> McpSessionListItem {
        McpSessionListItem {
            session_id: row.session_id,
            first_event_time: row.first_event_time,
            first_event_unix_ms: row.first_event_unix_ms,
            last_event_time: row.last_event_time,
            last_event_unix_ms: row.last_event_unix_ms,
            total_turns: row.total_turns,
            total_events: row.total_events,
            mode: Self::parse_mode(&row.mode),
            completed: row.completed != 0,
            title: non_empty_string(row.title),
            source: non_empty_string(row.source),
            harness: non_empty_string(row.harness),
            session_slug: non_empty_string(row.session_slug),
            session_summary: non_empty_string(row.session_summary),
        }
    }

    pub(super) fn map_session_metadata_row(row: SessionMetadataRow) -> SessionMetadata {
        SessionMetadata {
            session_id: row.session_id,
            first_event_time: row.first_event_time,
            first_event_unix_ms: row.first_event_unix_ms,
            last_event_time: row.last_event_time,
            last_event_unix_ms: row.last_event_unix_ms,
            total_turns: row.total_turns,
            total_events: row.total_events,
            user_messages: row.user_messages,
            assistant_messages: row.assistant_messages,
            tool_calls: row.tool_calls,
            tool_results: row.tool_results,
            mode: Self::parse_mode(&row.mode),
            first_event_uid: row.first_event_uid,
            last_event_uid: row.last_event_uid,
            last_actor_role: row.last_actor_role,
        }
    }

    pub(super) fn map_session_metadata_search_row(
        &self,
        rank: usize,
        row: SessionMetadataSearchRow,
        terms: &[String],
    ) -> SessionMetadataSearchHit {
        let has_session_summary = !row.first_event_time.is_empty();
        let snippet = evidence_snippet(
            &row.session_summary,
            &row.metadata_text,
            terms,
            self.cfg.preview_chars,
        );

        SessionMetadataSearchHit {
            rank,
            session_id: row.session_id,
            first_event_time: has_session_summary.then_some(row.first_event_time),
            first_event_unix_ms: has_session_summary.then_some(row.first_event_unix_ms),
            last_event_time: has_session_summary.then_some(row.last_event_time),
            last_event_unix_ms: has_session_summary.then_some(row.last_event_unix_ms),
            total_turns: has_session_summary.then_some(row.total_turns),
            total_events: has_session_summary.then_some(row.total_events),
            user_messages: has_session_summary.then_some(row.user_messages),
            assistant_messages: has_session_summary.then_some(row.assistant_messages),
            tool_calls: has_session_summary.then_some(row.tool_calls),
            tool_results: has_session_summary.then_some(row.tool_results),
            mode: (!row.mode.is_empty()).then(|| Self::parse_mode(&row.mode)),
            harness: non_empty_string(row.harness),
            inference_provider: non_empty_string(row.inference_provider),
            session_slug: non_empty_string(row.session_slug),
            session_summary: non_empty_string(row.session_summary),
            meta_event_uid: non_empty_string(row.meta_event_uid),
            score: row.score,
            matched_terms: row.matched_terms,
            snippet,
        }
    }

    pub(super) fn map_turn_row(row: TurnSummaryRow) -> TurnSummary {
        TurnSummary {
            session_id: row.session_id,
            turn_seq: row.turn_seq,
            turn_id: row.turn_id,
            started_at: row.started_at,
            started_at_unix_ms: row.started_at_unix_ms,
            ended_at: row.ended_at,
            ended_at_unix_ms: row.ended_at_unix_ms,
            total_events: row.total_events,
            user_messages: row.user_messages,
            assistant_messages: row.assistant_messages,
            tool_calls: row.tool_calls,
            tool_results: row.tool_results,
            reasoning_items: row.reasoning_items,
        }
    }

    pub(super) fn map_trace_event(row: TraceEventRow) -> TraceEvent {
        TraceEvent {
            session_id: row.session_id,
            event_uid: row.event_uid,
            event_order: row.event_order,
            turn_seq: row.turn_seq,
            event_time: row.event_time,
            event_unix_ms: row.event_unix_ms,
            actor_role: row.actor_role,
            event_class: row.event_class,
            payload_type: row.payload_type,
            call_id: row.call_id,
            name: row.name,
            phase: row.phase,
            item_id: row.item_id,
            source_ref: row.source_ref,
            text_content: row.text_content,
            payload_json: row.payload_json,
            token_usage_json: row.token_usage_json,
            endpoint_kind: row.endpoint_kind,
            token_usage_buckets: row.token_usage_buckets,
            token_usage_native_units: row.token_usage_native_units,
        }
    }
}
