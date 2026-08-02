use super::*;
use crate::domain::McpOpenSnapshot;
use std::collections::{BTreeMap as StdBTreeMap, HashMap as StdHashMap, HashSet};

const NAVIGATION_PAGE_SIZE: usize = 1024;
const HYDRATION_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone, Deserialize)]
struct NavRow {
    session_id: String,
    event_uid: String,
    event_version: u64,
    sort_time: String,
    source_file: String,
    source_generation: u32,
    source_offset: u64,
    source_line_no: u64,
    emission_index: u32,
    event_time: String,
    event_unix_ms: i64,
    event_kind: String,
    actor_kind: String,
    payload_type: String,
    turn_index: u32,
    tool_call_id: String,
    tool_name: String,
    phase: String,
    item_id: String,
    harness: String,
    inference_provider: String,
    source_name: String,
    is_user_message: u8,
    is_metadata_bearing: u8,
}

#[derive(Debug, Clone, Deserialize)]
struct HydratedRow {
    event_uid: String,
    source_ref: String,
    text_content: String,
    payload_json: String,
    token_usage_json: String,
    endpoint_kind: String,
    #[serde(default)]
    token_usage_buckets: BTreeMap<String, u64>,
    #[serde(default)]
    token_usage_native_units: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Copy)]
struct DerivedPosition {
    event_order: u64,
    turn_seq: u32,
    event_ordinal: u32,
}

#[derive(Default)]
struct TurnAccum {
    metadata: Option<TurnSummary>,
    first_event: Option<McpEventRef>,
    last_event: Option<McpEventRef>,
    user_input: Option<(NavRow, DerivedPosition)>,
    final_response: Option<(NavRow, DerivedPosition)>,
    tools_called: Vec<String>,
    tool_names: HashSet<String>,
    normalized_event_types: Vec<String>,
    event_types: HashSet<String>,
    completed: bool,
    terminal_event_uid: Option<String>,
    selected_events: Vec<(NavRow, DerivedPosition)>,
}

struct SessionScan {
    metadata: SessionMetadata,
    source: String,
    harness: String,
    inference_provider: String,
    omp_dispatch_title: String,
    turns: StdBTreeMap<u32, TurnAccum>,
    metadata_rows: Vec<(NavRow, DerivedPosition)>,
    completed: bool,
    terminal_event_uid: Option<String>,
    target_event: Option<(NavRow, DerivedPosition)>,
    previous_event: Option<McpEventRef>,
    next_event: Option<McpEventRef>,
    generation: u64,
}

impl ClickHouseConversationRepository {
    pub(super) async fn canonical_get_mcp_session(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<McpSessionOpen>> {
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }
        let scan = match self.scan_canonical_session(session_id, None, None).await? {
            Some(scan) => scan,
            None => return Ok(None),
        };
        let hydrated = self
            .hydrate_open_rows(session_id, hydration_uids(&scan, None))
            .await?;
        let (title, slug, summary) = session_labels(&scan, &hydrated);
        let turns = scan
            .turns
            .values()
            .map(|turn| compact_turn(turn, &hydrated, self.cfg.preview_chars))
            .collect();
        Ok(Some(McpSessionOpen {
            metadata: scan.metadata,
            title,
            source: non_empty(scan.source),
            harness: non_empty(scan.harness),
            inference_provider: non_empty(scan.inference_provider),
            session_slug: slug,
            session_summary: summary,
            turns,
            completed: scan.completed,
            terminal_event_uid: scan.terminal_event_uid,
            snapshot: Some(McpOpenSnapshot {
                slot: 0,
                generation: scan.generation,
            }),
        }))
    }

    pub(super) async fn canonical_get_mcp_turn(
        &self,
        session_id: &str,
        turn_seq: u32,
        include_events: bool,
    ) -> RepoResult<Option<McpTurnOpen>> {
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }
        let scan = match self
            .scan_canonical_session(session_id, Some(turn_seq), None)
            .await?
        {
            Some(scan) => scan,
            None => return Ok(None),
        };
        let Some(turn) = scan.turns.get(&turn_seq) else {
            return Ok(None);
        };
        let hydrated = self
            .hydrate_open_rows(
                session_id,
                hydration_uids(&scan, include_events.then_some(turn_seq)),
            )
            .await?;
        let compact = compact_turn(turn, &hydrated, self.cfg.preview_chars);
        let events = if include_events {
            turn.selected_events
                .iter()
                .filter_map(|(row, position)| {
                    hydrated
                        .get(&row.event_uid)
                        .map(|wide| event_summary(row, *position, wide, self.cfg.preview_chars))
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(Some(McpTurnOpen {
            metadata: compact.metadata,
            events,
            parent_session_source: non_empty(scan.source),
            user_input_summary: compact.user_input_summary,
            final_response_summary: compact.final_response_summary,
            user_input_event: compact.user_input_event,
            final_response_event: compact.final_response_event,
            tools_called: compact.tools_called,
            normalized_event_types: compact.normalized_event_types,
            completed: compact.completed,
            terminal_event_uid: compact.terminal_event_uid,
            previous_turn: adjacent_turn_ref(&scan.turns, turn_seq, false),
            next_turn: adjacent_turn_ref(&scan.turns, turn_seq, true),
            first_event: compact.first_event,
            last_event: compact.last_event,
            snapshot: Some(McpOpenSnapshot {
                slot: 0,
                generation: scan.generation,
            }),
        }))
    }

    pub(super) async fn canonical_get_mcp_event(
        &self,
        event_uid: &str,
    ) -> RepoResult<Option<McpEventOpen>> {
        let locator = self.table_ref("mcp_event_locator");
        #[derive(Deserialize)]
        struct OwnerRow {
            session_id: String,
        }
        let sql = format!(
            "SELECT session_id FROM {locator} FINAL WHERE event_uid = {} LIMIT 1 FORMAT JSONEachRow",
            sql_quote(event_uid)
        );
        let owners: Vec<OwnerRow> = self.map_backend(self.query_rows(&sql, None).await)?;
        let Some(owner) = owners.first() else {
            return Ok(None);
        };
        if !self.session_in_scope(&owner.session_id).await? {
            return Ok(None);
        }
        let scan = match self
            .scan_canonical_session(&owner.session_id, None, Some(event_uid))
            .await?
        {
            Some(scan) => scan,
            None => return Ok(None),
        };
        let Some((target, position)) = scan.target_event.as_ref() else {
            return Ok(None);
        };
        let hydrated = self
            .hydrate_open_rows(&owner.session_id, vec![event_uid.to_string()])
            .await?;
        let Some(wide) = hydrated.get(event_uid) else {
            return Ok(None);
        };
        let event = trace_event(target, *position, wide);
        let Some(parent_turn_accum) = scan.turns.get(&position.turn_seq) else {
            return Ok(None);
        };
        let Some(parent_turn) = parent_turn_accum.metadata.clone() else {
            return Ok(None);
        };
        Ok(Some(McpEventOpen {
            event,
            event_type: normalized_event_type(target),
            event_ordinal: position.event_ordinal,
            turn_completed: parent_turn_accum.completed,
            turn_terminal_event_uid: parent_turn_accum.terminal_event_uid.clone(),
            parent_session: scan.metadata,
            parent_session_source: non_empty(scan.source),
            parent_turn,
            previous_event: scan.previous_event,
            next_event: scan.next_event,
            previous_turn: adjacent_turn_ref(&scan.turns, position.turn_seq, false),
            next_turn: adjacent_turn_ref(&scan.turns, position.turn_seq, true),
        }))
    }

    async fn scan_canonical_session(
        &self,
        session_id: &str,
        selected_turn: Option<u32>,
        target_uid: Option<&str>,
    ) -> RepoResult<Option<SessionScan>> {
        let navigation = self.table_ref("mcp_event_navigation");
        let mut cursor: Option<NavRow> = None;
        let mut event_order = 0u64;
        let mut user_count = 0u32;
        let mut ordinals = StdHashMap::<u32, u32>::new();
        let mut turns = StdBTreeMap::<u32, TurnAccum>::new();
        let mut first: Option<McpEventRef> = None;
        let mut last: Option<McpEventRef> = None;
        let mut last_row_ref: Option<McpEventRef> = None;
        let mut target_event = None;
        let mut previous_event = None;
        let mut next_event = None;
        let mut metadata_rows = Vec::new();
        let mut counts = (0u64, 0u64, 0u64, 0u64);
        let mut source = String::new();
        let mut harness = String::new();
        let mut inference_provider = String::new();
        let mut mode_rank = 0u8;
        let mut session_completed = false;
        let mut last_actor_role = String::new();
        let mut session_terminal = None;
        let mut generation = 0xcbf29ce484222325u64;
        let mut omp_dispatch: Option<(String, String)> = None;

        loop {
            let after = cursor.as_ref().map(|row| {
                format!(
                    " AND (n.sort_time, n.source_file, n.source_generation, n.source_offset, n.source_line_no, n.emission_index, n.event_uid) > (toDateTime64({}, 3), {}, {}, {}, {}, {}, {})",
                    sql_quote(&row.sort_time),
                    sql_quote(&row.source_file),
                    row.source_generation,
                    row.source_offset,
                    row.source_line_no,
                    row.emission_index,
                    sql_quote(&row.event_uid)
                )
            }).unwrap_or_default();
            let sql = format!(
                "SELECT
  session_id, event_uid, toUInt64(event_version) AS event_version,
  toString(n.sort_time) AS sort_time, source_file, toUInt32(source_generation) AS source_generation,
  toUInt64(source_offset) AS source_offset, toUInt64(source_line_no) AS source_line_no,
  toUInt32(emission_index) AS emission_index,
  toString(display_time) AS event_time, toInt64(toUnixTimestamp64Milli(display_time)) AS event_unix_ms,
  event_kind, actor_kind, payload_type, toUInt32(turn_index) AS turn_index,
  tool_call_id, tool_name, if(tool_phase != '', tool_phase, op_status) AS phase,
  item_id, harness, inference_provider, source_name,
  toUInt8(is_user_message) AS is_user_message, toUInt8(is_metadata_bearing) AS is_metadata_bearing
FROM {navigation} AS n FINAL
WHERE n.session_id = {}{after}
ORDER BY n.sort_time, n.source_file, n.source_generation, n.source_offset, n.source_line_no, n.emission_index, n.event_uid
LIMIT {NAVIGATION_PAGE_SIZE}
FORMAT JSONEachRow",
                sql_quote(session_id)
            );
            let rows: Vec<NavRow> = self.map_backend(self.query_rows(&sql, None).await)?;
            if rows.is_empty() {
                break;
            }
            let row_count = rows.len();
            let next_cursor = rows.last().cloned();
            for row in rows {
                event_order = event_order.saturating_add(1);
                if row.is_user_message != 0 {
                    user_count = user_count.saturating_add(1);
                }
                let turn_seq = if row.turn_index > 0 {
                    row.turn_index
                } else {
                    user_count.max(1)
                };
                let event_ordinal = ordinals.entry(turn_seq).or_default();
                *event_ordinal = event_ordinal.saturating_add(1);
                let position = DerivedPosition {
                    event_order,
                    turn_seq,
                    event_ordinal: *event_ordinal,
                };
                update_generation(&mut generation, &row);
                let event_ref = nav_event_ref(&row, position);
                if first.as_ref().is_none_or(|current| {
                    (
                        row.event_time.as_str(),
                        position.event_order,
                        row.event_uid.as_str(),
                    ) < (
                        current.event_time.as_str(),
                        current.event_order,
                        current.event_uid.as_str(),
                    )
                }) {
                    first = Some(event_ref.clone());
                }
                if last.as_ref().is_none_or(|current| {
                    (
                        row.event_time.as_str(),
                        position.event_order,
                        row.event_uid.as_str(),
                    ) > (
                        current.event_time.as_str(),
                        current.event_order,
                        current.event_uid.as_str(),
                    )
                }) {
                    last = Some(event_ref.clone());
                    last_actor_role.clone_from(&row.actor_kind);
                }
                if target_uid == Some(row.event_uid.as_str()) {
                    target_event = Some((row.clone(), position));
                    previous_event = last_row_ref.clone();
                } else if target_event.is_some() && next_event.is_none() {
                    next_event = Some(event_ref.clone());
                }
                last_row_ref = Some(event_ref.clone());

                if row.actor_kind == "user" && row.event_kind == "message" {
                    counts.0 = counts.0.saturating_add(1);
                }
                if row.actor_kind == "assistant" && row.event_kind == "message" {
                    counts.1 = counts.1.saturating_add(1);
                }
                if row.event_kind == "tool_call" {
                    counts.2 = counts.2.saturating_add(1);
                }
                if row.event_kind == "tool_result" {
                    counts.3 = counts.3.saturating_add(1);
                }
                mode_rank = mode_rank.max(event_mode_rank(&row));
                if !row.source_name.is_empty() {
                    source.clone_from(&row.source_name);
                }
                if !row.harness.is_empty() {
                    harness.clone_from(&row.harness);
                }
                if !row.inference_provider.is_empty() {
                    inference_provider.clone_from(&row.inference_provider);
                }
                if row.source_name == "omp"
                    && row.source_file.ends_with(".jsonl")
                    && !row.source_file.ends_with(&format!("{session_id}.jsonl"))
                {
                    let title = row
                        .source_file
                        .replace('\\', "/")
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .trim_end_matches(".jsonl")
                        .to_string();
                    if !title.is_empty()
                        && omp_dispatch
                            .as_ref()
                            .is_none_or(|(time, _)| row.event_time < *time)
                    {
                        omp_dispatch = Some((row.event_time.clone(), title));
                    }
                }
                if row.is_metadata_bearing != 0 {
                    metadata_rows.push((row.clone(), position));
                }
                if is_terminal(&row) {
                    session_completed = row.payload_type == "task_complete";
                    session_terminal = Some(row.event_uid.clone());
                }

                let turn = turns.entry(turn_seq).or_default();
                update_turn(turn, &row, position, selected_turn == Some(turn_seq));
            }
            cursor = next_cursor;
            if row_count < NAVIGATION_PAGE_SIZE {
                break;
            }
        }

        let (Some(first), Some(last)) = (first, last) else {
            return Ok(None);
        };
        let metadata = SessionMetadata {
            session_id: session_id.to_string(),
            first_event_time: first.event_time.clone(),
            first_event_unix_ms: turns
                .values()
                .filter_map(|t| t.metadata.as_ref())
                .map(|t| t.started_at_unix_ms)
                .min()
                .unwrap_or_default(),
            last_event_time: last.event_time.clone(),
            last_event_unix_ms: turns
                .values()
                .filter_map(|t| t.metadata.as_ref())
                .map(|t| t.ended_at_unix_ms)
                .max()
                .unwrap_or_default(),
            total_turns: turns.keys().copied().max().unwrap_or_default(),
            total_events: event_order,
            user_messages: counts.0,
            assistant_messages: counts.1,
            tool_calls: counts.2,
            tool_results: counts.3,
            mode: mode_from_rank(mode_rank),
            first_event_uid: first.event_uid,
            last_event_uid: last.event_uid,
            last_actor_role,
        };
        Ok(Some(SessionScan {
            metadata,
            source,
            harness,
            inference_provider,
            omp_dispatch_title: omp_dispatch.map(|(_, title)| title).unwrap_or_default(),
            turns,
            metadata_rows,
            completed: session_completed,
            terminal_event_uid: session_terminal,
            target_event,
            previous_event,
            next_event,
            generation,
        }))
    }

    async fn hydrate_open_rows(
        &self,
        session_id: &str,
        mut event_uids: Vec<String>,
    ) -> RepoResult<StdHashMap<String, HydratedRow>> {
        event_uids.sort_unstable();
        event_uids.dedup();
        let events = self.table_ref("events");
        let mut hydrated = StdHashMap::with_capacity(event_uids.len());
        for chunk in event_uids.chunks(HYDRATION_BATCH_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let sql = format!(
                "SELECT event_uid, source_ref, text_content, payload_json, token_usage_json,
  endpoint_kind, token_usage_buckets, token_usage_native_units
FROM {events} FINAL
WHERE session_id = {} AND event_uid IN {}
FORMAT JSONEachRow",
                sql_quote(session_id),
                sql_array_strings(chunk)
            );
            let rows: Vec<HydratedRow> = self.map_backend(self.query_rows(&sql, None).await)?;
            hydrated.extend(rows.into_iter().map(|row| (row.event_uid.clone(), row)));
        }
        Ok(hydrated)
    }
}

fn update_turn(turn: &mut TurnAccum, row: &NavRow, position: DerivedPosition, selected: bool) {
    let event_ref = nav_event_ref(row, position);
    let metadata = turn.metadata.get_or_insert_with(|| TurnSummary {
        session_id: row.session_id.clone(),
        turn_seq: position.turn_seq,
        turn_id: position.turn_seq.to_string(),
        started_at: row.event_time.clone(),
        started_at_unix_ms: row.event_unix_ms,
        ended_at: row.event_time.clone(),
        ended_at_unix_ms: row.event_unix_ms,
        total_events: 0,
        user_messages: 0,
        assistant_messages: 0,
        tool_calls: 0,
        tool_results: 0,
        reasoning_items: 0,
    });
    if row.event_unix_ms < metadata.started_at_unix_ms {
        metadata.started_at.clone_from(&row.event_time);
        metadata.started_at_unix_ms = row.event_unix_ms;
    }
    if row.event_unix_ms > metadata.ended_at_unix_ms {
        metadata.ended_at.clone_from(&row.event_time);
        metadata.ended_at_unix_ms = row.event_unix_ms;
    }
    metadata.total_events = metadata.total_events.saturating_add(1);
    metadata.user_messages += u64::from(row.actor_kind == "user" && row.event_kind == "message");
    metadata.assistant_messages +=
        u64::from(row.actor_kind == "assistant" && row.event_kind == "message");
    metadata.tool_calls += u64::from(row.event_kind == "tool_call");
    metadata.tool_results += u64::from(row.event_kind == "tool_result");
    metadata.reasoning_items += u64::from(row.event_kind == "reasoning");
    if turn.first_event.is_none() {
        turn.first_event = Some(event_ref.clone());
    }
    turn.last_event = Some(event_ref);
    if ClickHouseConversationRepository::is_mcp_message_event(&row.event_kind, &row.payload_type)
        && row.actor_kind.eq_ignore_ascii_case("user")
        && turn.user_input.is_none()
    {
        turn.user_input = Some((row.clone(), position));
    }
    if ClickHouseConversationRepository::is_mcp_message_event(&row.event_kind, &row.payload_type)
        && row.actor_kind.eq_ignore_ascii_case("assistant")
        && !row.phase.eq_ignore_ascii_case("commentary")
    {
        turn.final_response = Some((row.clone(), position));
    }
    if row.event_kind == "tool_call"
        && !row.tool_name.is_empty()
        && turn.tool_names.insert(row.tool_name.clone())
    {
        turn.tools_called.push(row.tool_name.clone());
    }
    let event_type = normalized_event_type(row);
    if turn.event_types.insert(event_type.clone()) {
        turn.normalized_event_types.push(event_type);
    }
    if is_terminal(row) {
        turn.completed = row.payload_type == "task_complete";
        turn.terminal_event_uid = Some(row.event_uid.clone());
    }
    if selected {
        turn.selected_events.push((row.clone(), position));
    }
}

fn hydration_uids(scan: &SessionScan, include_turn: Option<u32>) -> Vec<String> {
    let mut uids = scan
        .metadata_rows
        .iter()
        .map(|(row, _)| row.event_uid.clone())
        .collect::<Vec<_>>();
    for (turn_seq, turn) in &scan.turns {
        if let Some((row, _)) = &turn.user_input {
            uids.push(row.event_uid.clone());
        }
        if let Some((row, _)) = &turn.final_response {
            uids.push(row.event_uid.clone());
        }
        if include_turn == Some(*turn_seq) {
            uids.extend(
                turn.selected_events
                    .iter()
                    .map(|(row, _)| row.event_uid.clone()),
            );
        }
    }
    uids
}

fn compact_turn(
    turn: &TurnAccum,
    hydrated: &StdHashMap<String, HydratedRow>,
    preview_chars: u16,
) -> McpTurnCompact {
    let user_input_summary = turn.user_input.as_ref().and_then(|(row, _)| {
        hydrated
            .get(&row.event_uid)
            .and_then(|wide| preview_from_wide(wide, preview_chars))
    });
    let final_response_summary = turn.final_response.as_ref().and_then(|(row, _)| {
        hydrated
            .get(&row.event_uid)
            .and_then(|wide| preview_from_wide(wide, preview_chars))
    });
    McpTurnCompact {
        metadata: turn
            .metadata
            .clone()
            .expect("turn accumulator always has metadata"),
        user_input_summary,
        final_response_summary,
        user_input_event: turn
            .user_input
            .as_ref()
            .map(|(row, pos)| nav_event_ref(row, *pos)),
        final_response_event: turn
            .final_response
            .as_ref()
            .map(|(row, pos)| nav_event_ref(row, *pos)),
        tools_called: turn.tools_called.clone(),
        normalized_event_types: turn.normalized_event_types.clone(),
        completed: turn.completed,
        terminal_event_uid: turn.terminal_event_uid.clone(),
        first_event: turn.first_event.clone(),
        last_event: turn.last_event.clone(),
    }
}

fn event_summary(
    row: &NavRow,
    position: DerivedPosition,
    wide: &HydratedRow,
    preview_chars: u16,
) -> McpEventSummary {
    McpEventSummary {
        session_id: row.session_id.clone(),
        event_uid: row.event_uid.clone(),
        event_order: position.event_order,
        turn_seq: position.turn_seq,
        event_time: row.event_time.clone(),
        event_unix_ms: row.event_unix_ms,
        actor_role: row.actor_kind.clone(),
        event_class: row.event_kind.clone(),
        payload_type: row.payload_type.clone(),
        event_type: normalized_event_type(row),
        call_id: row.tool_call_id.clone(),
        name: row.tool_name.clone(),
        phase: row.phase.clone(),
        text_preview: preview_from_wide(wide, preview_chars),
    }
}

fn trace_event(row: &NavRow, position: DerivedPosition, wide: &HydratedRow) -> TraceEvent {
    TraceEvent {
        session_id: row.session_id.clone(),
        event_uid: row.event_uid.clone(),
        event_order: position.event_order,
        turn_seq: position.turn_seq,
        event_time: row.event_time.clone(),
        event_unix_ms: row.event_unix_ms,
        actor_role: row.actor_kind.clone(),
        event_class: row.event_kind.clone(),
        payload_type: row.payload_type.clone(),
        call_id: row.tool_call_id.clone(),
        name: row.tool_name.clone(),
        phase: row.phase.clone(),
        item_id: row.item_id.clone(),
        source_ref: wide.source_ref.clone(),
        text_content: wide.text_content.clone(),
        payload_json: wide.payload_json.clone(),
        token_usage_json: wide.token_usage_json.clone(),
        endpoint_kind: wide.endpoint_kind.clone(),
        token_usage_buckets: wide.token_usage_buckets.clone(),
        token_usage_native_units: wide.token_usage_native_units.clone(),
    }
}

fn nav_event_ref(row: &NavRow, position: DerivedPosition) -> McpEventRef {
    McpEventRef {
        session_id: row.session_id.clone(),
        event_uid: row.event_uid.clone(),
        event_order: position.event_order,
        turn_seq: position.turn_seq,
        event_time: row.event_time.clone(),
        event_type: normalized_event_type(row),
    }
}

fn adjacent_turn_ref(
    turns: &StdBTreeMap<u32, TurnAccum>,
    turn_seq: u32,
    next: bool,
) -> Option<McpTurnRef> {
    let turn = if next {
        turns.range((turn_seq.saturating_add(1))..).next()
    } else {
        turns.range(..turn_seq).next_back()
    }?;
    let metadata = turn.1.metadata.as_ref()?;
    Some(McpTurnRef {
        session_id: metadata.session_id.clone(),
        turn_seq: metadata.turn_seq,
        turn_id: metadata.turn_id.clone(),
        started_at: metadata.started_at.clone(),
        ended_at: metadata.ended_at.clone(),
    })
}

fn session_labels(
    scan: &SessionScan,
    hydrated: &StdHashMap<String, HydratedRow>,
) -> (Option<String>, Option<String>, Option<String>) {
    let mut ordered = scan
        .metadata_rows
        .iter()
        .filter_map(|(row, _)| hydrated.get(&row.event_uid).map(|wide| (row, wide)))
        .collect::<Vec<_>>();
    ordered.sort_by(|(a, _), (b, _)| {
        (a.event_time.as_str(), a.event_uid.as_str())
            .cmp(&(b.event_time.as_str(), b.event_uid.as_str()))
    });
    let mut title = String::new();
    let mut name = String::new();
    let mut summary = String::new();
    let mut slug = String::new();
    for (row, wide) in ordered {
        let payload: Value = serde_json::from_str(&wide.payload_json).unwrap_or(Value::Null);
        if let Some(value) = payload
            .get("title")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
        {
            title = value.to_string();
        }
        if row.event_kind == "session_meta" {
            if let Some(value) = payload
                .get("name")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                name = value.to_string();
            }
            if let Some(value) = payload
                .get("summary")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                summary = value.to_string();
            }
            if let Some(value) = payload
                .get("slug")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                slug = value.to_string();
            }
        }
    }
    let resolved_title = if scan.source == "omp" {
        first_non_empty(&[&title, &name, &summary, &scan.omp_dispatch_title])
    } else {
        first_non_empty(&[&title, &name])
    };
    let resolved_summary = if scan.source == "omp" {
        first_non_empty(&[&summary, &resolved_title, &name, &scan.omp_dispatch_title])
    } else {
        first_non_empty(&[&summary, &resolved_title, &name])
    };
    (
        non_empty(resolved_title),
        non_empty(slug),
        non_empty(resolved_summary),
    )
}

fn preview_from_wide(row: &HydratedRow, preview_chars: u16) -> Option<String> {
    if row.text_content.trim().is_empty() {
        compact_source(&row.payload_json, true, preview_chars)
    } else {
        compact_source(&row.text_content, false, preview_chars)
    }
}

fn compact_source(source: &str, is_payload: bool, preview_chars: u16) -> Option<String> {
    if source.trim().is_empty() {
        return None;
    }
    let output_limit = usize::from(preview_chars).max(1);
    let source_limit = if is_payload {
        output_limit.max(4).saturating_mul(2)
    } else {
        output_limit.max(4)
    };
    let truncated = if source.chars().count() <= source_limit {
        source.to_string()
    } else {
        format!(
            "{}...",
            source
                .chars()
                .take(source_limit.saturating_sub(3))
                .collect::<String>()
        )
    };
    let compact = compact_text_line(&truncated, output_limit);
    (!compact.is_empty()).then_some(compact)
}

fn normalized_event_type(row: &NavRow) -> String {
    ClickHouseConversationRepository::mcp_event_type_for(
        &row.event_kind,
        &row.payload_type,
        &row.actor_kind,
    )
    .as_str()
    .to_string()
}

fn is_terminal(row: &NavRow) -> bool {
    matches!(row.payload_type.as_str(), "task_complete" | "turn_aborted")
}

fn event_mode_rank(row: &NavRow) -> u8 {
    if matches!(
        row.payload_type.as_str(),
        "web_search_call" | "search_results_received"
    ) || (row.payload_type == "tool_use"
        && matches!(row.tool_name.as_str(), "WebSearch" | "WebFetch"))
    {
        3
    } else if row.source_name == "codex-mcp"
        || ClickHouseConversationRepository::is_mcp_internal_tool_name(&row.tool_name)
    {
        2
    } else if matches!(row.event_kind.as_str(), "tool_call" | "tool_result")
        || row.payload_type == "tool_use"
    {
        1
    } else {
        0
    }
}

fn mode_from_rank(rank: u8) -> ConversationMode {
    match rank {
        3 => ConversationMode::WebSearch,
        2 => ConversationMode::McpInternal,
        1 => ConversationMode::ToolCalling,
        _ => ConversationMode::Chat,
    }
}

fn update_generation(hash: &mut u64, row: &NavRow) {
    for byte in row
        .event_uid
        .as_bytes()
        .iter()
        .copied()
        .chain(row.event_version.to_le_bytes())
    {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x100000001b3);
    }
}

fn first_non_empty(values: &[&str]) -> String {
    values
        .iter()
        .find(|value| !value.is_empty())
        .map(|value| (*value).to_string())
        .unwrap_or_default()
}

fn non_empty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}
