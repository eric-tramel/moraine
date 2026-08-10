use super::*;
use crate::domain::{
    CanonicalContinuation, CanonicalReadAnchor, CanonicalReadOutcome, CanonicalSessionPage,
    CanonicalSessionSignals, CanonicalTurnPage, McpOpenSnapshot,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use std::collections::{BTreeMap as StdBTreeMap, HashMap as StdHashMap, HashSet};
use std::io::{Read, Write};

const NAVIGATION_PAGE_SIZE: usize = 1024;
const HYDRATION_BATCH_SIZE: usize = 256;
const SESSION_SEEK_BATCH_SIZE: usize = 256;
const CARRY_NAME_CHARS: usize = 120;
const CARRY_PREVIEW_CHARS: usize = 240;
const CARRY_TOOL_LIMIT: usize = 26;
const CARRY_DECOMPRESSED_MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Deserialize)]
struct NavRow {
    session_id: String,
    event_uid: String,
    event_version: u64,
    sort_time: String,
    #[serde(default)]
    sort_unix_ms: i64,
    source_file: String,
    source_generation: u32,
    source_offset: u64,
    source_line_no: u64,
    emission_index: u32,
    event_time: String,
    event_unix_ms: i64,
    #[serde(default)]
    content_unix_ms: i64,
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
    #[serde(default)]
    has_session_title: u8,
    #[serde(default)]
    has_session_name: u8,
    #[serde(default)]
    has_session_summary: u8,
    #[serde(default)]
    has_session_slug: u8,
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

#[derive(Debug, Clone, Deserialize)]
struct SeekRow {
    session_id: String,
    sort_time: String,
    sort_unix_ms: i64,
    source_file: String,
    source_generation: u32,
    source_offset: u64,
    source_line_no: u64,
    emission_index: u32,
    event_uid: String,
    event_version: u64,
    event_time: String,
    event_unix_ms: i64,
    #[serde(default)]
    content_unix_ms: i64,
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

#[derive(Deserialize)]
struct VersionRow {
    event_uid: String,
    event_version: u64,
    session_id: String,
    sort_time: String,
    source_file: String,
    source_generation: u32,
    source_offset: u64,
    source_line_no: u64,
    emission_index: u32,
}

#[derive(Deserialize)]
struct SignalRow {
    unique_versions: u64,
    fingerprint_a: u64,
    fingerprint_b: u64,
}

#[derive(Debug, Clone, Copy)]
struct DerivedPosition {
    event_order: u64,
    turn_seq: u32,
    event_ordinal: u32,
    prefix_user_message_count: u64,
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
    explicit_rows: u64,
    implicit_rows: u64,
    completed: bool,
    terminal_event_uid: Option<String>,
    selected_events: Vec<(NavRow, DerivedPosition)>,
    last_nav: Option<CanonicalReadAnchor>,
}

struct SessionScan {
    metadata: SessionMetadata,
    source: String,
    harness: String,
    inference_provider: String,
    omp_dispatch_title: String,
    turns: StdBTreeMap<u32, TurnAccum>,
    metadata_rows: Vec<(NavRow, DerivedPosition)>,
    prefix_anchors: StdBTreeMap<u64, CanonicalReadAnchor>,
    completed: bool,
    terminal_event_uid: Option<String>,
    target_event: Option<(NavRow, DerivedPosition)>,
    previous_event: Option<McpEventRef>,
    next_event: Option<McpEventRef>,
    explicit_rows: u64,
    implicit_rows: u64,
    generation: u64,
}

#[derive(serde::Serialize, Deserialize)]
struct SessionPageCarry {
    header: McpSessionOpen,
    first_turn_seq: Option<u32>,
    last_turn_seq: Option<u32>,
    all_explicit: bool,
    all_implicit: bool,
}

#[derive(serde::Serialize, Deserialize)]
struct TurnPageCarry {
    header: McpTurnOpen,
    all_explicit: bool,
    all_implicit: bool,
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
            .hydrate_open_rows(
                &owner.session_id,
                vec![(
                    event_uid.to_string(),
                    target.event_version,
                    target.content_unix_ms,
                )],
            )
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
        let navigation = self.table_ref("mcp_event_navigation_seek");
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
        let mut prefix_anchors = StdBTreeMap::new();
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
                    " AND (n.sort_time, n.source_file, n.source_generation, n.source_offset, n.source_line_no, n.emission_index, n.event_uid, n.event_version) > (toDateTime64({}, 3), {}, {}, {}, {}, {}, {}, {})",
                    sql_quote(&row.sort_time),
                    sql_quote(&row.source_file),
                    row.source_generation,
                    row.source_offset,
                    row.source_line_no,
                    row.emission_index,
                    sql_quote(&row.event_uid),
                    row.event_version
                )
            }).unwrap_or_default();
            let sql = format!(
                "SELECT
  session_id, event_uid, toUInt64(event_version) AS event_version,
  toString(n.sort_time) AS sort_time, toInt64(toUnixTimestamp64Milli(n.sort_time)) AS sort_unix_ms,
  source_file, toUInt32(source_generation) AS source_generation,
  toUInt64(source_offset) AS source_offset, toUInt64(source_line_no) AS source_line_no,
  toUInt32(emission_index) AS emission_index,
  toString(display_time) AS event_time, toInt64(toUnixTimestamp64Milli(display_time)) AS event_unix_ms,
  toInt64(toUnixTimestamp64Milli(event_ts)) AS content_unix_ms,
  event_kind, actor_kind, payload_type, toUInt32(turn_index) AS turn_index,
  tool_call_id, tool_name, if(tool_phase != '', tool_phase, op_status) AS phase,
  item_id, harness, inference_provider, source_name,
  toUInt8(is_user_message) AS is_user_message, toUInt8(is_metadata_bearing) AS is_metadata_bearing,
  toUInt8(has_session_title) AS has_session_title, toUInt8(has_session_name) AS has_session_name,
  toUInt8(has_session_summary) AS has_session_summary, toUInt8(has_session_slug) AS has_session_slug
FROM {navigation} AS n FINAL
WHERE n.session_id = {}{after}
ORDER BY n.sort_time, n.source_file, n.source_generation, n.source_offset, n.source_line_no, n.emission_index, n.event_uid, n.event_version
LIMIT {NAVIGATION_PAGE_SIZE}
FORMAT JSONEachRow",
                sql_quote(session_id)
            );
            let candidates: Vec<NavRow> = self.map_backend(self.query_rows(&sql, None).await)?;
            if candidates.is_empty() {
                break;
            }
            let row_count = candidates.len();
            let next_cursor = candidates.last().cloned();
            let uids = candidates
                .iter()
                .map(|row| row.event_uid.clone())
                .collect::<Vec<_>>();
            let current = self.current_version_rows(&uids).await?;
            let rows = candidates.into_iter().filter(|candidate| {
                current
                    .get(&candidate.event_uid)
                    .is_some_and(|latest| version_matches_nav(latest, candidate, session_id))
            });
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
                    prefix_user_message_count: u64::from(user_count),
                };
                prefix_anchors.insert(u64::from(user_count), canonical_anchor(&row, position));
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
        let explicit_rows = turns.values().map(|turn| turn.explicit_rows).sum();
        let implicit_rows = turns.values().map(|turn| turn.implicit_rows).sum();
        Ok(Some(SessionScan {
            metadata,
            source,
            harness,
            inference_provider,
            omp_dispatch_title: omp_dispatch.map(|(_, title)| title).unwrap_or_default(),
            turns,
            metadata_rows,
            prefix_anchors,
            completed: session_completed,
            terminal_event_uid: session_terminal,
            target_event,
            previous_event,
            next_event,
            generation,
            explicit_rows,
            implicit_rows,
        }))
    }

    async fn canonical_seek_signals(
        &self,
        session_id: &str,
    ) -> RepoResult<Option<CanonicalSessionSignals>> {
        let signals = self.table_ref("mcp_session_mutation_signals");
        let sql = format!(
            "SELECT
  toUInt64(uniqExactMerge(unique_versions)) AS unique_versions,
  toUInt64(sumDistinctMerge(fingerprint_a)) AS fingerprint_a,
  toUInt64(sumDistinctMerge(fingerprint_b)) AS fingerprint_b
FROM {signals}
WHERE session_id = {}
GROUP BY session_id
FORMAT JSONEachRow",
            sql_quote(session_id)
        );
        let rows: Vec<SignalRow> = self.map_backend(self.query_rows(&sql, None).await)?;
        Ok(rows.first().map(|row| CanonicalSessionSignals {
            unique_versions: row.unique_versions,
            fingerprint_a: row.fingerprint_a,
            fingerprint_b: row.fingerprint_b,
        }))
    }

    async fn chronological_seek_rows(
        &self,
        session_id: &str,
        after: &CanonicalReadAnchor,
        row_limit: usize,
    ) -> RepoResult<Vec<SeekRow>> {
        let seek = self.table_ref("mcp_event_navigation_seek");
        let sql = format!(
            "SELECT
  s.session_id AS session_id, toString(s.sort_time) AS sort_time,
  toInt64(toUnixTimestamp64Milli(s.sort_time)) AS sort_unix_ms,
  s.source_file AS source_file, toUInt32(s.source_generation) AS source_generation,
  toUInt64(s.source_offset) AS source_offset, toUInt64(s.source_line_no) AS source_line_no,
  toUInt32(s.emission_index) AS emission_index, s.event_uid AS event_uid,
  toUInt64(s.event_version) AS event_version, toString(s.display_time) AS event_time,
  toInt64(toUnixTimestamp64Milli(s.display_time)) AS event_unix_ms,
  toInt64(toUnixTimestamp64Milli(s.event_ts)) AS content_unix_ms,
  s.event_kind AS event_kind, s.actor_kind AS actor_kind, s.payload_type AS payload_type,
  toUInt32(s.turn_index) AS turn_index, s.tool_call_id AS tool_call_id,
  s.tool_name AS tool_name, if(s.tool_phase != '', s.tool_phase, s.op_status) AS phase,
  s.item_id AS item_id, s.harness AS harness, s.inference_provider AS inference_provider,
  s.source_name AS source_name, toUInt8(s.is_user_message) AS is_user_message,
  toUInt8(s.is_metadata_bearing) AS is_metadata_bearing
FROM {seek} AS s
WHERE s.session_id = {} AND
  (s.sort_time, s.source_file, s.source_generation, s.source_offset, s.source_line_no, s.emission_index, s.event_uid, s.event_version) >
  (fromUnixTimestamp64Milli({}), {}, {}, {}, {}, {}, {}, {})
ORDER BY s.sort_time, s.source_file, s.source_generation, s.source_offset, s.source_line_no, s.emission_index, s.event_uid, s.event_version
LIMIT {row_limit}
FORMAT JSONEachRow",
            sql_quote(session_id),
            after.sort_time_ms,
            sql_quote(&after.source_file),
            after.source_generation,
            after.source_offset,
            after.source_line_no,
            after.emission_index,
            sql_quote(&after.event_uid),
            after.event_version,
        );
        self.map_backend(self.query_rows(&sql, None).await)
    }

    async fn explicit_turn_seek_rows(
        &self,
        session_id: &str,
        after_turn_seq: u32,
        turn_limit: u16,
    ) -> RepoResult<Vec<SeekRow>> {
        let seek = self.table_ref("mcp_event_turn_seek");
        let turn_limit_plus_one = usize::from(turn_limit).saturating_add(1);
        let sql = format!(
            "SELECT
  s.session_id AS session_id, toString(s.sort_time) AS sort_time,
  toInt64(toUnixTimestamp64Milli(s.sort_time)) AS sort_unix_ms,
  s.source_file AS source_file, toUInt32(s.source_generation) AS source_generation,
  toUInt64(s.source_offset) AS source_offset, toUInt64(s.source_line_no) AS source_line_no,
  toUInt32(s.emission_index) AS emission_index, s.event_uid AS event_uid,
  toUInt64(s.event_version) AS event_version, toString(s.display_time) AS event_time,
  toInt64(toUnixTimestamp64Milli(s.display_time)) AS event_unix_ms,
  toInt64(toUnixTimestamp64Milli(s.event_ts)) AS content_unix_ms,
  s.event_kind AS event_kind, s.actor_kind AS actor_kind, s.payload_type AS payload_type,
  toUInt32(s.turn_index) AS turn_index, s.tool_call_id AS tool_call_id,
  s.tool_name AS tool_name, if(s.tool_phase != '', s.tool_phase, s.op_status) AS phase,
  '' AS item_id, '' AS harness, '' AS inference_provider, '' AS source_name,
  toUInt8(s.is_user_message) AS is_user_message, toUInt8(0) AS is_metadata_bearing
FROM {seek} AS s
WHERE s.session_id = {} AND s.turn_index IN (
  SELECT t.turn_index
  FROM {seek} AS t
  WHERE t.session_id = {} AND t.turn_index > {}
  GROUP BY t.turn_index
  ORDER BY t.turn_index
  LIMIT {turn_limit_plus_one}
)
ORDER BY s.turn_index, s.sort_time, s.source_file, s.source_generation, s.source_offset, s.source_line_no, s.emission_index, s.event_uid, s.event_version
FORMAT JSONEachRow",
            sql_quote(session_id),
            sql_quote(session_id),
            after_turn_seq,
        );
        self.map_backend(self.query_rows(&sql, None).await)
    }

    async fn explicit_event_seek_rows(
        &self,
        session_id: &str,
        turn_seq: u32,
        after: &CanonicalReadAnchor,
        limit: u16,
    ) -> RepoResult<Vec<SeekRow>> {
        let seek = self.table_ref("mcp_event_turn_seek");
        let row_limit = usize::from(limit).saturating_add(1);
        let sql = format!(
            "SELECT
  s.session_id AS session_id, toString(s.sort_time) AS sort_time,
  toInt64(toUnixTimestamp64Milli(s.sort_time)) AS sort_unix_ms,
  s.source_file AS source_file, toUInt32(s.source_generation) AS source_generation,
  toUInt64(s.source_offset) AS source_offset, toUInt64(s.source_line_no) AS source_line_no,
  toUInt32(s.emission_index) AS emission_index, s.event_uid AS event_uid,
  toUInt64(s.event_version) AS event_version, toString(s.display_time) AS event_time,
  toInt64(toUnixTimestamp64Milli(s.display_time)) AS event_unix_ms,
  toInt64(toUnixTimestamp64Milli(s.event_ts)) AS content_unix_ms,
  s.event_kind AS event_kind, s.actor_kind AS actor_kind, s.payload_type AS payload_type,
  toUInt32(s.turn_index) AS turn_index, s.tool_call_id AS tool_call_id,
  s.tool_name AS tool_name, if(s.tool_phase != '', s.tool_phase, s.op_status) AS phase,
  '' AS item_id, '' AS harness, '' AS inference_provider, '' AS source_name,
  toUInt8(s.is_user_message) AS is_user_message, toUInt8(0) AS is_metadata_bearing
FROM {seek} AS s
WHERE s.session_id = {} AND s.turn_index = {} AND
  (s.sort_time, s.source_file, s.source_generation, s.source_offset, s.source_line_no, s.emission_index, s.event_uid, s.event_version) >
  (fromUnixTimestamp64Milli({}), {}, {}, {}, {}, {}, {}, {})
ORDER BY s.turn_index, s.sort_time, s.source_file, s.source_generation, s.source_offset, s.source_line_no, s.emission_index, s.event_uid, s.event_version
LIMIT {row_limit}
FORMAT JSONEachRow",
            sql_quote(session_id),
            turn_seq,
            after.sort_time_ms,
            sql_quote(&after.source_file),
            after.source_generation,
            after.source_offset,
            after.source_line_no,
            after.emission_index,
            sql_quote(&after.event_uid),
            after.event_version,
        );
        self.map_backend(self.query_rows(&sql, None).await)
    }

    async fn reconciled_explicit_turn_rows(
        &self,
        session_id: &str,
        after_turn_seq: u32,
        limit: u16,
    ) -> RepoResult<Vec<NavRow>> {
        let wanted = usize::from(limit).saturating_add(1);
        let mut seek_after = after_turn_seq;
        let mut rows = Vec::new();
        let mut current_turns = HashSet::new();
        loop {
            let candidates = self
                .explicit_turn_seek_rows(session_id, seek_after, limit)
                .await?;
            if candidates.is_empty() {
                break;
            }
            let raw_turns = candidates
                .iter()
                .map(|row| row.turn_index)
                .collect::<HashSet<_>>();
            let next_after = raw_turns.iter().copied().max().unwrap_or(seek_after);
            let current = self.reconcile_seek_rows(session_id, candidates).await?;
            for row in current {
                current_turns.insert(row.turn_index);
                rows.push(row);
            }
            if current_turns.len() >= wanted || raw_turns.len() < wanted || next_after <= seek_after
            {
                break;
            }
            seek_after = next_after;
        }
        rows.sort_by(|left, right| canonical_row_key(left).cmp(&canonical_row_key(right)));
        rows.dedup_by(|left, right| {
            left.event_uid == right.event_uid && left.event_version == right.event_version
        });
        Ok(rows)
    }

    async fn reconciled_explicit_event_rows(
        &self,
        session_id: &str,
        turn_seq: u32,
        after: &CanonicalReadAnchor,
        limit: u16,
    ) -> RepoResult<Vec<NavRow>> {
        let wanted = usize::from(limit).saturating_add(1);
        let mut seek_after = after.clone();
        let mut rows = Vec::new();
        loop {
            let candidates = self
                .explicit_event_seek_rows(session_id, turn_seq, &seek_after, limit)
                .await?;
            if candidates.is_empty() {
                break;
            }
            let candidate_count = candidates.len();
            let raw_last = candidates.last().cloned();
            rows.extend(self.reconcile_seek_rows(session_id, candidates).await?);
            rows.sort_by(|left, right| canonical_row_key(left).cmp(&canonical_row_key(right)));
            rows.dedup_by(|left, right| {
                left.event_uid == right.event_uid && left.event_version == right.event_version
            });
            if rows.len() >= wanted || candidate_count < wanted {
                break;
            }
            let Some(raw_last) = raw_last else {
                break;
            };
            let next_after = seek_anchor(
                &raw_last,
                seek_after.event_order,
                seek_after.prefix_user_message_count,
                seek_after.event_ordinal,
            );
            if next_after == seek_after {
                break;
            }
            seek_after = next_after;
        }
        rows.sort_by(|left, right| canonical_row_key(left).cmp(&canonical_row_key(right)));
        rows.dedup_by(|left, right| {
            left.event_uid == right.event_uid && left.event_version == right.event_version
        });
        Ok(rows)
    }

    async fn current_version_rows(
        &self,
        uids: &[String],
    ) -> RepoResult<StdHashMap<String, VersionRow>> {
        let versions = self.table_ref("mcp_event_version_seek");
        let sql = format!(
            "SELECT
  event_uid, toUInt64(event_version) AS event_version, session_id,
  toString(sort_time) AS sort_time, source_file,
  toUInt32(source_generation) AS source_generation,
  toUInt64(source_offset) AS source_offset, toUInt64(source_line_no) AS source_line_no,
  toUInt32(emission_index) AS emission_index
FROM {versions}
WHERE event_uid IN {}
ORDER BY event_uid, event_version
FORMAT JSONEachRow",
            sql_array_strings(uids),
        );
        let current: Vec<VersionRow> = self.map_backend(self.query_rows(&sql, None).await)?;
        Ok(current
            .into_iter()
            .map(|row| (row.event_uid.clone(), row))
            .collect())
    }

    async fn reconcile_seek_rows(
        &self,
        session_id: &str,
        candidates: Vec<SeekRow>,
    ) -> RepoResult<Vec<NavRow>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let uids = candidates
            .iter()
            .map(|row| row.event_uid.clone())
            .collect::<Vec<_>>();
        let by_uid = self.current_version_rows(&uids).await?;
        let mut rows = candidates
            .into_iter()
            .filter_map(|candidate| {
                let current = by_uid.get(&candidate.event_uid)?;
                if current.session_id != session_id
                    || current.event_version != candidate.event_version
                    || current.sort_time != candidate.sort_time
                    || current.source_file != candidate.source_file
                    || current.source_generation != candidate.source_generation
                    || current.source_offset != candidate.source_offset
                    || current.source_line_no != candidate.source_line_no
                    || current.emission_index != candidate.emission_index
                {
                    return None;
                }
                Some(NavRow {
                    session_id: candidate.session_id,
                    event_uid: candidate.event_uid,
                    event_version: candidate.event_version,
                    sort_time: candidate.sort_time,
                    sort_unix_ms: candidate.sort_unix_ms,
                    source_file: candidate.source_file,
                    source_generation: candidate.source_generation,
                    source_offset: candidate.source_offset,
                    source_line_no: candidate.source_line_no,
                    emission_index: candidate.emission_index,
                    event_time: candidate.event_time,
                    event_unix_ms: candidate.event_unix_ms,
                    content_unix_ms: candidate.content_unix_ms,
                    event_kind: candidate.event_kind,
                    actor_kind: candidate.actor_kind,
                    payload_type: candidate.payload_type,
                    turn_index: candidate.turn_index,
                    tool_call_id: candidate.tool_call_id,
                    tool_name: candidate.tool_name,
                    phase: candidate.phase,
                    item_id: candidate.item_id,
                    harness: candidate.harness,
                    inference_provider: candidate.inference_provider,
                    source_name: candidate.source_name,
                    is_user_message: candidate.is_user_message,
                    is_metadata_bearing: candidate.is_metadata_bearing,
                    has_session_title: 0,
                    has_session_name: 0,
                    has_session_summary: 0,
                    has_session_slug: 0,
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| canonical_row_key(left).cmp(&canonical_row_key(right)));
        rows.dedup_by(|left, right| {
            left.event_uid == right.event_uid && left.event_version == right.event_version
        });
        Ok(rows)
    }

    async fn seek_session_continuation(
        &self,
        session_id: &str,
        limit: u16,
        cursor: &CanonicalContinuation,
        carry: &SessionPageCarry,
        signals: CanonicalSessionSignals,
    ) -> RepoResult<CanonicalReadOutcome<CanonicalSessionPage>> {
        let explicit = if carry.all_implicit {
            Vec::new()
        } else {
            self.reconciled_explicit_turn_rows(session_id, cursor.after_turn_seq, limit)
                .await?
        };
        let mut rows = explicit
            .into_iter()
            .map(|row| {
                let turn_seq = row.turn_index;
                (row, turn_seq, cursor.after.prefix_user_message_count)
            })
            .collect::<Vec<_>>();

        let mut query_after = cursor.after.clone();
        let mut event_order = cursor.after.event_order;
        let mut user_count = cursor.after.prefix_user_message_count;
        let mut implicit_turns = HashSet::new();
        let mut prefix_anchors = StdBTreeMap::new();
        let wanted_implicit = usize::from(limit).saturating_add(1);
        'seek: loop {
            let candidates = self
                .chronological_seek_rows(session_id, &query_after, SESSION_SEEK_BATCH_SIZE)
                .await?;
            if candidates.is_empty() {
                break;
            }
            let candidate_count = candidates.len();
            let raw_last = candidates.last().cloned();
            let current = self.reconcile_seek_rows(session_id, candidates).await?;
            for row in current {
                event_order = event_order.saturating_add(1);
                user_count = user_count.saturating_add(u64::from(row.is_user_message != 0));
                let implicit_turn = u32::try_from(user_count.max(1)).unwrap_or(u32::MAX);
                let position = DerivedPosition {
                    event_order,
                    turn_seq: if row.turn_index > 0 {
                        row.turn_index
                    } else {
                        implicit_turn
                    },
                    event_ordinal: 0,
                    prefix_user_message_count: user_count,
                };
                if row.turn_index == 0 && implicit_turn > cursor.after_turn_seq {
                    if implicit_turns.insert(implicit_turn)
                        && implicit_turns.len() > wanted_implicit
                    {
                        break 'seek;
                    }
                    rows.push((row.clone(), implicit_turn, user_count));
                }
                prefix_anchors.insert(user_count, canonical_anchor(&row, position));
                query_after = canonical_anchor(&row, position);
            }
            if candidate_count < SESSION_SEEK_BATCH_SIZE {
                break;
            }
            if let Some(raw_last) = raw_last {
                query_after = seek_anchor(&raw_last, event_order, user_count, 0);
            }
        }

        rows.sort_by(|(left, _, _), (right, _, _)| {
            canonical_row_key(left).cmp(&canonical_row_key(right))
        });
        rows.dedup_by(|(left, _, _), (right, _, _)| {
            left.event_uid == right.event_uid && left.event_version == right.event_version
        });
        let mut turns = StdBTreeMap::<u32, TurnAccum>::new();
        let mut ordinals = StdHashMap::<u32, u32>::new();
        let mut page_event_order = cursor.after.event_order;
        for (row, turn_seq, prefix_user_message_count) in rows {
            if turn_seq <= cursor.after_turn_seq {
                continue;
            }
            page_event_order = page_event_order.saturating_add(1);
            let event_ordinal = ordinals.entry(turn_seq).or_default();
            *event_ordinal = event_ordinal.saturating_add(1);
            update_turn(
                turns.entry(turn_seq).or_default(),
                &row,
                DerivedPosition {
                    event_order: page_event_order,
                    turn_seq,
                    event_ordinal: *event_ordinal,
                    prefix_user_message_count,
                },
                false,
            );
        }
        let selected = turns
            .iter()
            .take(usize::from(limit).saturating_add(1))
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Ok(CanonicalReadOutcome::Reopen);
        }
        let has_more = selected.len() > usize::from(limit);
        let page = &selected[..selected.len().min(usize::from(limit))];
        let mut hydration = Vec::with_capacity(page.len().saturating_mul(2));
        for (_, turn) in page {
            if let Some((row, _)) = &turn.user_input {
                hydration.push((
                    row.event_uid.clone(),
                    row.event_version,
                    row.content_unix_ms,
                ));
            }
            if let Some((row, _)) = &turn.final_response {
                hydration.push((
                    row.event_uid.clone(),
                    row.event_version,
                    row.content_unix_ms,
                ));
            }
        }
        let hydrated = self.hydrate_open_rows(session_id, hydration).await?;
        if self.canonical_seek_signals(session_id).await?.as_ref() != Some(&signals) {
            return Ok(CanonicalReadOutcome::Reopen);
        }
        let mut session = carry.header.clone();
        session.metadata.session_id = session_id.to_string();
        session.turns = page
            .iter()
            .map(|(_, turn)| compact_turn(turn, &hydrated, self.cfg.preview_chars))
            .collect();
        let continuation = if has_more {
            page.last().map(|(turn_seq, turn)| {
                let after = prefix_anchors
                    .range(..=u64::from(**turn_seq))
                    .next_back()
                    .map(|(_, anchor)| anchor.clone())
                    .unwrap_or_else(|| cursor.after.clone());
                let mut after = after;
                after.event_ordinal = turn
                    .last_nav
                    .as_ref()
                    .map_or(0, |anchor| anchor.event_ordinal);
                CanonicalContinuation {
                    signals,
                    after,
                    after_turn_seq: **turn_seq,
                    session_carry: cursor.session_carry.clone(),
                }
            })
        } else {
            None
        };
        Ok(CanonicalReadOutcome::Page(CanonicalSessionPage {
            session,
            first_turn_seq: carry.first_turn_seq,
            last_turn_seq: carry.last_turn_seq,
            continuation,
        }))
    }

    pub(super) async fn canonical_open_session_page_impl(
        &self,
        session_id: &str,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalSessionPage>>> {
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }
        if let Some(cursor) = after.as_ref() {
            let carry = cursor
                .session_carry
                .as_deref()
                .and_then(decode_carry::<SessionPageCarry>);
            if let Some(carry) = carry.as_ref().filter(|carry| carry.all_explicit) {
                let Some(signals) = self.canonical_seek_signals(session_id).await? else {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                };
                if cursor.signals != signals {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                }
                let rows = self
                    .reconciled_explicit_turn_rows(session_id, cursor.after_turn_seq, limit)
                    .await?;
                let mut turns = StdBTreeMap::<u32, TurnAccum>::new();
                let mut ordinals = StdHashMap::<u32, u32>::new();
                let mut event_order = cursor.after.event_order;
                let mut user_count = cursor.after.prefix_user_message_count;
                for row in rows {
                    event_order = event_order.saturating_add(1);
                    user_count = user_count.saturating_add(u64::from(row.is_user_message != 0));
                    let turn_seq = row.turn_index;
                    let event_ordinal = ordinals.entry(turn_seq).or_default();
                    *event_ordinal = event_ordinal.saturating_add(1);
                    let position = DerivedPosition {
                        event_order,
                        turn_seq,
                        event_ordinal: *event_ordinal,
                        prefix_user_message_count: user_count,
                    };
                    update_turn(turns.entry(turn_seq).or_default(), &row, position, false);
                }
                let selected = turns
                    .iter()
                    .take(usize::from(limit) + 1)
                    .collect::<Vec<_>>();
                if selected.is_empty() {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                }
                let has_more = selected.len() > usize::from(limit);
                let page = &selected[..selected.len().min(usize::from(limit))];
                let mut hydration = Vec::with_capacity(page.len().saturating_mul(2));
                for (_, turn) in page {
                    if let Some((row, _)) = &turn.user_input {
                        hydration.push((
                            row.event_uid.clone(),
                            row.event_version,
                            row.content_unix_ms,
                        ));
                    }
                    if let Some((row, _)) = &turn.final_response {
                        hydration.push((
                            row.event_uid.clone(),
                            row.event_version,
                            row.content_unix_ms,
                        ));
                    }
                }
                let hydrated = self.hydrate_open_rows(session_id, hydration).await?;
                if self.canonical_seek_signals(session_id).await?.as_ref() != Some(&signals) {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                }
                let mut session = carry.header.clone();
                session.metadata.session_id = session_id.to_string();
                session.turns = page
                    .iter()
                    .map(|(_, turn)| compact_turn(turn, &hydrated, self.cfg.preview_chars))
                    .collect();
                let continuation = if has_more {
                    page.last().and_then(|(turn_seq, turn)| {
                        turn.last_nav.as_ref().map(|after| CanonicalContinuation {
                            signals: signals.clone(),
                            after: after.clone(),
                            after_turn_seq: **turn_seq,
                            session_carry: cursor.session_carry.clone(),
                        })
                    })
                } else {
                    None
                };
                return Ok(Some(CanonicalReadOutcome::Page(CanonicalSessionPage {
                    session,
                    first_turn_seq: carry.first_turn_seq,
                    last_turn_seq: carry.last_turn_seq,
                    continuation,
                })));
            }
            let carry = cursor
                .session_carry
                .as_deref()
                .and_then(decode_carry::<SessionPageCarry>);
            if let Some(carry) = carry {
                return self
                    .seek_session_continuation(
                        session_id,
                        limit,
                        cursor,
                        &carry,
                        cursor.signals.clone(),
                    )
                    .await
                    .map(Some);
            }
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }

        let signals_before = self.canonical_seek_signals(session_id).await?;
        let scan = match self.scan_canonical_session(session_id, None, None).await? {
            Some(scan) => scan,
            None => return Ok(None),
        };
        let Some(signals) = self.canonical_seek_signals(session_id).await? else {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        };
        if signals_before.as_ref() != Some(&signals) {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }
        let after_turn_seq = after.as_ref().map_or(0, |cursor| cursor.after_turn_seq);
        let selected = scan
            .turns
            .range(after_turn_seq.saturating_add(1)..)
            .take(usize::from(limit).saturating_add(1))
            .collect::<Vec<_>>();
        if after.is_some() && selected.is_empty() {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }
        let has_more = selected.len() > usize::from(limit);
        let page = &selected[..selected.len().min(usize::from(limit))];
        let mut hydration = metadata_hydration_rows(&scan.metadata_rows);
        for (_, turn) in page {
            if let Some((row, _)) = &turn.user_input {
                hydration.push((
                    row.event_uid.clone(),
                    row.event_version,
                    row.content_unix_ms,
                ));
            }
            if let Some((row, _)) = &turn.final_response {
                hydration.push((
                    row.event_uid.clone(),
                    row.event_version,
                    row.content_unix_ms,
                ));
            }
        }
        let hydrated = self.hydrate_open_rows(session_id, hydration).await?;
        if self.canonical_seek_signals(session_id).await?.as_ref() != Some(&signals) {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }
        let (title, slug, summary) = session_labels(&scan, &hydrated);
        let turns = page
            .iter()
            .map(|(_, turn)| compact_turn(turn, &hydrated, self.cfg.preview_chars))
            .collect::<Vec<_>>();
        let first_turn = scan.turns.values().find_map(turn_accum_ref);
        let last_turn = scan.turns.values().rev().find_map(turn_accum_ref);
        let mut session = McpSessionOpen {
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
            snapshot: None,
        };
        let all_explicit = scan.implicit_rows == 0;
        let carry = SessionPageCarry {
            header: compact_session_carry_header(&session),
            first_turn_seq: first_turn.as_ref().map(|turn| turn.turn_seq),
            last_turn_seq: last_turn.as_ref().map(|turn| turn.turn_seq),
            all_explicit,
            all_implicit: scan.explicit_rows == 0,
        };
        let carry = encode_carry(&carry);
        let continuation = if has_more {
            page.last().and_then(|(turn_seq, turn)| {
                let anchor = if all_explicit {
                    turn.last_nav.clone()
                } else {
                    scan.prefix_anchors
                        .range(..=u64::from(**turn_seq))
                        .next_back()
                        .map(|(_, anchor)| anchor.clone())
                };
                anchor.map(|after| CanonicalContinuation {
                    signals: signals.clone(),
                    after,
                    after_turn_seq: **turn_seq,
                    session_carry: carry.clone(),
                })
            })
        } else {
            None
        };
        session.snapshot = None;
        Ok(Some(CanonicalReadOutcome::Page(CanonicalSessionPage {
            session,
            first_turn_seq: first_turn.as_ref().map(|turn| turn.turn_seq),
            last_turn_seq: last_turn.as_ref().map(|turn| turn.turn_seq),

            continuation,
        })))
    }
    async fn seek_turn_continuation(
        &self,
        session_id: &str,
        turn_seq: u32,
        limit: u16,
        cursor: &CanonicalContinuation,
        carry: &TurnPageCarry,
        signals: CanonicalSessionSignals,
    ) -> RepoResult<CanonicalReadOutcome<CanonicalTurnPage>> {
        let explicit = if carry.all_implicit {
            Vec::new()
        } else {
            self.reconciled_explicit_event_rows(session_id, turn_seq, &cursor.after, limit)
                .await?
        };
        let mut prefixes = StdHashMap::new();
        let mut rows = explicit;
        let mut query_after = cursor.after.clone();
        let mut event_order = cursor.after.event_order;
        let mut user_count = cursor.after.prefix_user_message_count;
        let wanted = usize::from(limit).saturating_add(1);
        let mut implicit_count = 0usize;
        'seek: loop {
            let candidates = self
                .chronological_seek_rows(session_id, &query_after, wanted)
                .await?;
            if candidates.is_empty() {
                break;
            }
            let candidate_count = candidates.len();
            let raw_last = candidates.last().cloned();
            let current = self.reconcile_seek_rows(session_id, candidates).await?;
            for row in current {
                event_order = event_order.saturating_add(1);
                user_count = user_count.saturating_add(u64::from(row.is_user_message != 0));
                prefixes.insert(row.event_uid.clone(), user_count);
                let implicit_turn = u32::try_from(user_count.max(1)).unwrap_or(u32::MAX);
                if row.turn_index == 0 {
                    if implicit_turn > turn_seq || implicit_count >= wanted {
                        break 'seek;
                    }
                    if implicit_turn == turn_seq {
                        implicit_count = implicit_count.saturating_add(1);
                        rows.push(row.clone());
                    }
                }
                query_after = canonical_anchor(
                    &row,
                    DerivedPosition {
                        event_order,
                        turn_seq: if row.turn_index > 0 {
                            row.turn_index
                        } else {
                            implicit_turn
                        },
                        event_ordinal: 0,
                        prefix_user_message_count: user_count,
                    },
                );
            }
            if candidate_count < wanted {
                break;
            }
            if let Some(raw_last) = raw_last {
                query_after = seek_anchor(&raw_last, event_order, user_count, 0);
            }
        }
        rows.sort_by(|left, right| canonical_row_key(left).cmp(&canonical_row_key(right)));
        rows.dedup_by(|left, right| {
            left.event_uid == right.event_uid && left.event_version == right.event_version
        });
        if rows.is_empty() {
            return Ok(CanonicalReadOutcome::Reopen);
        }
        let has_more = rows.len() > usize::from(limit);
        rows.truncate(usize::from(limit));
        let mut page_turn = TurnAccum::default();
        let mut page_event_order = cursor.after.event_order;
        let mut event_ordinal = cursor.after.event_ordinal;
        for row in rows {
            page_event_order = page_event_order.saturating_add(1);
            event_ordinal = event_ordinal.saturating_add(1);
            update_turn(
                &mut page_turn,
                &row,
                DerivedPosition {
                    event_order: page_event_order,
                    turn_seq,
                    event_ordinal,
                    prefix_user_message_count: prefixes
                        .get(&row.event_uid)
                        .copied()
                        .unwrap_or(cursor.after.prefix_user_message_count),
                },
                true,
            );
        }
        let hydration = page_turn
            .selected_events
            .iter()
            .map(|(row, _)| {
                (
                    row.event_uid.clone(),
                    row.event_version,
                    row.content_unix_ms,
                )
            })
            .collect();
        let hydrated = self.hydrate_open_rows(session_id, hydration).await?;
        if self.canonical_seek_signals(session_id).await?.as_ref() != Some(&signals) {
            return Ok(CanonicalReadOutcome::Reopen);
        }
        let mut turn = carry.header.clone();
        turn.metadata.session_id = session_id.to_string();
        turn.metadata.turn_seq = turn_seq;
        turn.metadata.turn_id = turn_seq.to_string();
        restore_turn_carry_identity(&mut turn, session_id);
        turn.events = page_turn
            .selected_events
            .iter()
            .filter_map(|(row, position)| {
                hydrated
                    .get(&row.event_uid)
                    .map(|wide| event_summary(row, *position, wide, self.cfg.preview_chars))
            })
            .collect();
        merge_page_summaries(&mut turn, &page_turn, &hydrated, self.cfg.preview_chars);
        let continuation = if has_more {
            page_turn
                .selected_events
                .last()
                .map(|(row, position)| CanonicalContinuation {
                    signals,
                    after: canonical_anchor(row, *position),
                    after_turn_seq: turn_seq,
                    session_carry: cursor.session_carry.clone(),
                })
        } else {
            None
        };
        Ok(CanonicalReadOutcome::Page(CanonicalTurnPage {
            turn,
            continuation,
        }))
    }

    pub(super) async fn canonical_open_turn_page_impl(
        &self,
        session_id: &str,
        turn_seq: u32,
        limit: u16,
        after: Option<CanonicalContinuation>,
    ) -> RepoResult<Option<CanonicalReadOutcome<CanonicalTurnPage>>> {
        if !self.session_in_scope(session_id).await? {
            return Ok(None);
        }
        if let Some(cursor) = after.as_ref() {
            let carry = cursor
                .session_carry
                .as_deref()
                .and_then(decode_carry::<TurnPageCarry>);
            if let Some(carry) = carry.filter(|carry| carry.all_explicit) {
                let Some(signals) = self.canonical_seek_signals(session_id).await? else {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                };
                if cursor.signals != signals || cursor.after_turn_seq != turn_seq {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                }
                let rows = self
                    .reconciled_explicit_event_rows(session_id, turn_seq, &cursor.after, limit)
                    .await?;
                let mut page_turn = TurnAccum::default();
                let mut event_order = cursor.after.event_order;
                let mut event_ordinal = cursor.after.event_ordinal;
                let mut user_count = cursor.after.prefix_user_message_count;
                for row in rows.into_iter().take(usize::from(limit) + 1) {
                    event_order = event_order.saturating_add(1);
                    event_ordinal = event_ordinal.saturating_add(1);
                    user_count = user_count.saturating_add(u64::from(row.is_user_message != 0));
                    let position = DerivedPosition {
                        event_order,
                        turn_seq,
                        event_ordinal,
                        prefix_user_message_count: user_count,
                    };
                    update_turn(&mut page_turn, &row, position, true);
                }
                if page_turn.selected_events.is_empty() {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                }
                let has_more = page_turn.selected_events.len() > usize::from(limit);
                page_turn.selected_events.truncate(usize::from(limit));
                let hydration = page_turn
                    .selected_events
                    .iter()
                    .map(|(row, _)| {
                        (
                            row.event_uid.clone(),
                            row.event_version,
                            row.content_unix_ms,
                        )
                    })
                    .collect();
                let hydrated = self.hydrate_open_rows(session_id, hydration).await?;
                if self.canonical_seek_signals(session_id).await?.as_ref() != Some(&signals) {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                }
                let mut turn = carry.header.clone();
                turn.metadata.session_id = session_id.to_string();
                turn.metadata.turn_seq = turn_seq;
                turn.metadata.turn_id = turn_seq.to_string();
                restore_turn_carry_identity(&mut turn, session_id);
                turn.events = page_turn
                    .selected_events
                    .iter()
                    .filter_map(|(row, position)| {
                        hydrated
                            .get(&row.event_uid)
                            .map(|wide| event_summary(row, *position, wide, self.cfg.preview_chars))
                    })
                    .collect();
                merge_page_summaries(&mut turn, &page_turn, &hydrated, self.cfg.preview_chars);
                let continuation = if has_more {
                    page_turn
                        .selected_events
                        .last()
                        .map(|(row, position)| CanonicalContinuation {
                            signals,
                            after: canonical_anchor(row, *position),
                            after_turn_seq: turn_seq,
                            session_carry: cursor.session_carry.clone(),
                        })
                } else {
                    None
                };
                return Ok(Some(CanonicalReadOutcome::Page(CanonicalTurnPage {
                    turn,
                    continuation,
                })));
            }
            let carry = cursor
                .session_carry
                .as_deref()
                .and_then(decode_carry::<TurnPageCarry>);
            if let Some(carry) = carry {
                if cursor.after_turn_seq != turn_seq {
                    return Ok(Some(CanonicalReadOutcome::Reopen));
                }
                return self
                    .seek_turn_continuation(
                        session_id,
                        turn_seq,
                        limit,
                        cursor,
                        &carry,
                        cursor.signals.clone(),
                    )
                    .await
                    .map(Some);
            }
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }

        let signals_before = self.canonical_seek_signals(session_id).await?;
        let scan = match self
            .scan_canonical_session(session_id, Some(turn_seq), None)
            .await?
        {
            Some(scan) => scan,
            None => return Ok(None),
        };
        let Some(signals) = self.canonical_seek_signals(session_id).await? else {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        };
        if signals_before.as_ref() != Some(&signals) {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }
        let Some(turn_accum) = scan.turns.get(&turn_seq) else {
            return Ok(None);
        };
        let end = usize::from(limit).min(turn_accum.selected_events.len());
        let page_rows = &turn_accum.selected_events[..end];
        let has_more = end < turn_accum.selected_events.len();
        let hydration = page_rows
            .iter()
            .map(|(row, _)| {
                (
                    row.event_uid.clone(),
                    row.event_version,
                    row.content_unix_ms,
                )
            })
            .collect();
        let hydrated = self.hydrate_open_rows(session_id, hydration).await?;
        if self.canonical_seek_signals(session_id).await?.as_ref() != Some(&signals) {
            return Ok(Some(CanonicalReadOutcome::Reopen));
        }
        let compact = compact_turn(turn_accum, &hydrated, self.cfg.preview_chars);
        let events = page_rows
            .iter()
            .filter_map(|(row, position)| {
                hydrated
                    .get(&row.event_uid)
                    .map(|wide| event_summary(row, *position, wide, self.cfg.preview_chars))
            })
            .collect();
        let turn = McpTurnOpen {
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
            snapshot: None,
        };
        let carry = TurnPageCarry {
            header: compact_turn_carry_header(&turn),
            all_explicit: turn_accum.implicit_rows == 0,
            all_implicit: turn_accum.explicit_rows == 0,
        };
        let carry = encode_carry(&carry);
        let continuation = if has_more {
            page_rows
                .last()
                .map(|(row, position)| CanonicalContinuation {
                    signals,
                    after: canonical_anchor(row, *position),
                    after_turn_seq: turn_seq,
                    session_carry: carry,
                })
        } else {
            None
        };
        Ok(Some(CanonicalReadOutcome::Page(CanonicalTurnPage {
            turn,
            continuation,
        })))
    }

    async fn hydrate_open_rows(
        &self,
        session_id: &str,
        mut event_keys: Vec<(String, u64, i64)>,
    ) -> RepoResult<StdHashMap<String, HydratedRow>> {
        event_keys.sort_unstable();
        event_keys.dedup();
        let events = self.table_ref("events");
        let mut hydrated = StdHashMap::with_capacity(event_keys.len());
        for chunk in event_keys.chunks(HYDRATION_BATCH_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let min_event_ts_ms = chunk
                .iter()
                .map(|(_, _, event_ts_ms)| *event_ts_ms)
                .min()
                .unwrap_or(0);
            let max_event_ts_ms = chunk
                .iter()
                .map(|(_, _, event_ts_ms)| *event_ts_ms)
                .max()
                .unwrap_or(0);
            let sql = format!(
                "SELECT event_uid, source_ref, text_content, payload_json, token_usage_json,
  endpoint_kind, token_usage_buckets, token_usage_native_units
FROM {events}
PREWHERE session_id = {} AND event_uid IN {}
WHERE event_ts >= fromUnixTimestamp64Milli({})
  AND event_ts <= fromUnixTimestamp64Milli({})
  AND (event_uid, event_version) IN ({})
FORMAT JSONEachRow",
                sql_quote(session_id),
                sql_event_uids(chunk),
                min_event_ts_ms,
                max_event_ts_ms,
                sql_event_version_tuples(chunk)
            );
            let rows: Vec<HydratedRow> = self.map_backend(self.query_rows(&sql, None).await)?;
            hydrated.extend(rows.into_iter().map(|row| (row.event_uid.clone(), row)));
        }
        Ok(hydrated)
    }
}

fn encode_carry<T: serde::Serialize>(carry: &T) -> Option<String> {
    let json = serde_json::to_vec(carry).ok()?;
    if json.len() as u64 > CARRY_DECOMPRESSED_MAX_BYTES {
        return None;
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&json).ok()?;
    let compressed = encoder.finish().ok()?;
    Some(URL_SAFE_NO_PAD.encode(compressed))
}

fn decode_carry<T: serde::de::DeserializeOwned>(carry: &str) -> Option<T> {
    let compressed = URL_SAFE_NO_PAD.decode(carry).ok()?;
    let decoder = ZlibDecoder::new(compressed.as_slice());
    let mut json = Vec::new();
    decoder
        .take(CARRY_DECOMPRESSED_MAX_BYTES.saturating_add(1))
        .read_to_end(&mut json)
        .ok()?;
    if json.len() as u64 > CARRY_DECOMPRESSED_MAX_BYTES {
        return None;
    }
    serde_json::from_slice(&json).ok()
}

fn canonical_row_key(row: &NavRow) -> (&str, &str, u32, u64, u64, u32, &str, u64) {
    (
        row.sort_time.as_str(),
        row.source_file.as_str(),
        row.source_generation,
        row.source_offset,
        row.source_line_no,
        row.emission_index,
        row.event_uid.as_str(),
        row.event_version,
    )
}
fn canonical_anchor(row: &NavRow, position: DerivedPosition) -> CanonicalReadAnchor {
    CanonicalReadAnchor {
        sort_time_ms: row.sort_unix_ms,
        source_file: row.source_file.clone(),
        source_generation: row.source_generation,
        source_offset: row.source_offset,
        source_line_no: row.source_line_no,

        emission_index: row.emission_index,
        event_uid: row.event_uid.clone(),
        event_version: row.event_version,
        event_order: position.event_order,
        prefix_user_message_count: position.prefix_user_message_count,
        event_ordinal: position.event_ordinal,
    }
}
fn version_matches_nav(current: &VersionRow, candidate: &NavRow, session_id: &str) -> bool {
    current.session_id == session_id
        && current.event_version == candidate.event_version
        && current.sort_time == candidate.sort_time
        && current.source_file == candidate.source_file
        && current.source_generation == candidate.source_generation
        && current.source_offset == candidate.source_offset
        && current.source_line_no == candidate.source_line_no
        && current.emission_index == candidate.emission_index
}
fn seek_anchor(
    row: &SeekRow,
    event_order: u64,
    prefix_user_message_count: u64,
    event_ordinal: u32,
) -> CanonicalReadAnchor {
    CanonicalReadAnchor {
        sort_time_ms: row.sort_unix_ms,
        source_file: row.source_file.clone(),
        source_generation: row.source_generation,
        source_offset: row.source_offset,
        source_line_no: row.source_line_no,
        emission_index: row.emission_index,
        event_uid: row.event_uid.clone(),
        event_version: row.event_version,
        event_order,
        prefix_user_message_count,
        event_ordinal,
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
    if row.turn_index > 0 {
        turn.explicit_rows = turn.explicit_rows.saturating_add(1);
    } else {
        turn.implicit_rows = turn.implicit_rows.saturating_add(1);
    }
    turn.last_nav = Some(canonical_anchor(row, position));
}

fn hydration_uids(scan: &SessionScan, include_turn: Option<u32>) -> Vec<(String, u64, i64)> {
    let mut uids = scan
        .metadata_rows
        .iter()
        .map(|(row, _)| {
            (
                row.event_uid.clone(),
                row.event_version,
                row.content_unix_ms,
            )
        })
        .collect::<Vec<_>>();
    for (turn_seq, turn) in &scan.turns {
        if let Some((row, _)) = &turn.user_input {
            uids.push((
                row.event_uid.clone(),
                row.event_version,
                row.content_unix_ms,
            ));
        }
        if let Some((row, _)) = &turn.final_response {
            uids.push((
                row.event_uid.clone(),
                row.event_version,
                row.content_unix_ms,
            ));
        }
        if include_turn == Some(*turn_seq) {
            uids.extend(turn.selected_events.iter().map(|(row, _)| {
                (
                    row.event_uid.clone(),
                    row.event_version,
                    row.content_unix_ms,
                )
            }));
        }
    }
    uids
}
fn sql_event_uids(keys: &[(String, u64, i64)]) -> String {
    format!(
        "[{}]",
        keys.iter()
            .map(|(event_uid, _, _)| sql_quote(event_uid))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn sql_event_version_tuples(keys: &[(String, u64, i64)]) -> String {
    keys.iter()
        .map(|(event_uid, event_version, _)| format!("({}, {event_version})", sql_quote(event_uid)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn compact_optional_carry(value: Option<&str>, max_chars: usize) -> Option<String> {
    value
        .map(|value| compact_text_line(value, max_chars))
        .filter(|value| !value.is_empty())
}

fn compact_carry_event_ref(event: Option<&McpEventRef>) -> Option<McpEventRef> {
    event.map(|event| McpEventRef {
        session_id: String::new(),
        event_uid: compact_text_line(&event.event_uid, CARRY_NAME_CHARS),
        event_order: event.event_order,
        turn_seq: event.turn_seq,
        event_time: String::new(),
        event_type: String::new(),
    })
}

fn compact_carry_turn_ref(turn: Option<&McpTurnRef>) -> Option<McpTurnRef> {
    turn.map(|turn| McpTurnRef {
        session_id: String::new(),
        turn_seq: turn.turn_seq,
        turn_id: String::new(),
        started_at: String::new(),
        ended_at: String::new(),
    })
}

fn compact_session_carry_header(session: &McpSessionOpen) -> McpSessionOpen {
    let mut metadata = session.metadata.clone();
    metadata.session_id = compact_text_line(&metadata.session_id, CARRY_NAME_CHARS);
    metadata.first_event_time = compact_text_line(&metadata.first_event_time, CARRY_NAME_CHARS);
    metadata.last_event_time = compact_text_line(&metadata.last_event_time, CARRY_NAME_CHARS);
    metadata.first_event_uid = compact_text_line(&metadata.first_event_uid, CARRY_NAME_CHARS);
    metadata.last_event_uid = compact_text_line(&metadata.last_event_uid, CARRY_NAME_CHARS);
    metadata.last_actor_role = compact_text_line(&metadata.last_actor_role, CARRY_NAME_CHARS);
    McpSessionOpen {
        metadata,
        title: compact_optional_carry(session.title.as_deref(), CARRY_PREVIEW_CHARS),
        source: compact_optional_carry(session.source.as_deref(), CARRY_NAME_CHARS),
        harness: compact_optional_carry(session.harness.as_deref(), CARRY_NAME_CHARS),
        inference_provider: compact_optional_carry(
            session.inference_provider.as_deref(),
            CARRY_NAME_CHARS,
        ),
        session_slug: compact_optional_carry(session.session_slug.as_deref(), CARRY_PREVIEW_CHARS),
        session_summary: compact_optional_carry(
            session.session_summary.as_deref(),
            CARRY_PREVIEW_CHARS,
        ),
        turns: Vec::new(),
        completed: session.completed,
        terminal_event_uid: compact_optional_carry(
            session.terminal_event_uid.as_deref(),
            CARRY_NAME_CHARS,
        ),
        snapshot: None,
    }
}

fn compact_turn_carry_header(turn: &McpTurnOpen) -> McpTurnOpen {
    let mut metadata = turn.metadata.clone();
    metadata.session_id = compact_text_line(&metadata.session_id, CARRY_NAME_CHARS);
    metadata.turn_id = compact_text_line(&metadata.turn_id, CARRY_NAME_CHARS);
    metadata.started_at = compact_text_line(&metadata.started_at, CARRY_NAME_CHARS);
    metadata.ended_at = compact_text_line(&metadata.ended_at, CARRY_NAME_CHARS);

    McpTurnOpen {
        metadata,
        events: Vec::new(),
        parent_session_source: compact_optional_carry(
            turn.parent_session_source.as_deref(),
            CARRY_NAME_CHARS,
        ),
        user_input_summary: compact_optional_carry(
            turn.user_input_summary.as_deref(),
            CARRY_PREVIEW_CHARS,
        ),
        final_response_summary: compact_optional_carry(
            turn.final_response_summary.as_deref(),
            CARRY_PREVIEW_CHARS,
        ),
        user_input_event: compact_carry_event_ref(turn.user_input_event.as_ref()),
        final_response_event: compact_carry_event_ref(turn.final_response_event.as_ref()),
        tools_called: turn
            .tools_called
            .iter()
            .take(CARRY_TOOL_LIMIT)
            .map(|value| compact_text_line(value, CARRY_NAME_CHARS))
            .collect(),
        normalized_event_types: turn.normalized_event_types.clone(),
        completed: turn.completed,
        terminal_event_uid: compact_optional_carry(
            turn.terminal_event_uid.as_deref(),
            CARRY_NAME_CHARS,
        ),
        previous_turn: compact_carry_turn_ref(turn.previous_turn.as_ref()),
        next_turn: compact_carry_turn_ref(turn.next_turn.as_ref()),
        first_event: compact_carry_event_ref(turn.first_event.as_ref()),
        last_event: compact_carry_event_ref(turn.last_event.as_ref()),
        snapshot: None,
    }
}
fn restore_turn_carry_identity(turn: &mut McpTurnOpen, session_id: &str) {
    for event in [
        turn.user_input_event.as_mut(),
        turn.final_response_event.as_mut(),
        turn.first_event.as_mut(),
        turn.last_event.as_mut(),
    ]
    .into_iter()
    .flatten()
    {
        event.session_id = session_id.to_string();
    }
    for adjacent in [turn.previous_turn.as_mut(), turn.next_turn.as_mut()]
        .into_iter()
        .flatten()
    {
        adjacent.session_id = session_id.to_string();
    }
}

fn merge_page_summaries(
    turn: &mut McpTurnOpen,
    page: &TurnAccum,
    hydrated: &StdHashMap<String, HydratedRow>,
    preview_chars: u16,
) {
    let compact = compact_turn(page, hydrated, preview_chars);
    if compact.user_input_summary.is_some() {
        turn.user_input_summary = compact.user_input_summary;
        turn.user_input_event = compact.user_input_event;
    }
    if compact.final_response_summary.is_some() {
        turn.final_response_summary = compact.final_response_summary;
        turn.final_response_event = compact.final_response_event;
    }
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

fn turn_accum_ref(turn: &TurnAccum) -> Option<McpTurnRef> {
    turn.metadata.as_ref().map(|metadata| McpTurnRef {
        session_id: metadata.session_id.clone(),
        turn_seq: metadata.turn_seq,
        turn_id: metadata.turn_id.clone(),
        started_at: metadata.started_at.clone(),
        ended_at: metadata.ended_at.clone(),
    })
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

fn metadata_hydration_rows(rows: &[(NavRow, DerivedPosition)]) -> Vec<(String, u64, i64)> {
    let winner = |has_value: fn(&NavRow) -> bool| {
        rows.iter()
            .map(|(row, _)| row)
            .filter(|row| has_value(row))
            .max_by(|left, right| {
                (left.event_time.as_str(), left.event_uid.as_str())
                    .cmp(&(right.event_time.as_str(), right.event_uid.as_str()))
            })
    };
    let mut seen = HashSet::new();
    [
        winner(|row| row.has_session_title != 0),
        winner(|row| row.has_session_name != 0),
        winner(|row| row.has_session_summary != 0),
        winner(|row| row.has_session_slug != 0),
    ]
    .into_iter()
    .flatten()
    .filter(|row| seen.insert(row.event_uid.as_str()))
    .map(|row| {
        (
            row.event_uid.clone(),
            row.event_version,
            row.content_unix_ms,
        )
    })
    .collect()
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
