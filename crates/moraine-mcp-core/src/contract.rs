use chrono::DateTime;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Value};
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

const SESSION_ID_PREFIX: &str = "session:";
const TURN_ID_PREFIX: &str = "turn:";
const EVENT_ID_PREFIX: &str = "event:";
const BASE64_URL_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

pub const SEARCH_SESSIONS_TOOL: &str = "search_sessions";
pub const OPEN_TOOL: &str = "open";
pub const LIST_SESSIONS_TOOL: &str = "list_sessions";
pub const SEARCH_SESSIONS_SCHEMA_VERSION: &str = "moraine.mcp.search_sessions.v1";
pub const OPEN_SCHEMA_VERSION: &str = "moraine.mcp.open.v1";
pub const LIST_SESSIONS_SCHEMA_VERSION: &str = "moraine.mcp.list_sessions.v1";
pub const ERROR_SCHEMA_VERSION: &str = "moraine.mcp.error.v1";
pub const SEARCH_SESSIONS_DEFAULT_N_HITS: u16 = 10;
pub const SEARCH_SESSIONS_MIN_N_HITS: u16 = 1;
pub const SEARCH_SESSIONS_MAX_N_HITS: u16 = 50;
pub const SEARCH_SESSIONS_MAX_QUERY_CHARS: usize = 4096;
pub const SEARCH_SESSIONS_SLA_TARGET_MS: u64 = 750;
pub const OPEN_SLA_TARGET_MS: u64 = 500;
pub const LIST_SESSIONS_DEFAULT_LIMIT: u16 = 20;
pub const LIST_SESSIONS_MIN_LIMIT: u16 = 1;
pub const LIST_SESSIONS_DEFAULT_SLA_TARGET_MS: u64 = 300;
pub const LIST_SESSIONS_BROAD_SLA_TARGET_MS: u64 = 1_000;
pub const LIST_SESSIONS_FILTERED_BROAD_SLA_TARGET_MS: u64 = 1_200;
pub const LIST_SESSIONS_DEADLINE_MS: u64 = 3_000;
pub const FILE_ATTENTION_TOOL: &str = "file_attention";
pub const FILE_ATTENTION_SCHEMA_VERSION: &str = "moraine.mcp.file_attention.v1";
pub const FILE_ATTENTION_MIN_LIMIT: u16 = 1;
pub const FILE_ATTENTION_DEFAULT_LIMIT: u16 = 50;
pub const FILE_ATTENTION_DEFAULT_SLA_TARGET_MS: u64 = 600;
pub const FILE_ATTENTION_BROAD_SLA_TARGET_MS: u64 = 1_200;
pub const FILE_ATTENTION_DEADLINE_MS: u64 = 4_000;
/// Minimum number of non-empty, slash-separated segments a path tail must have
/// before it is treated as specific enough to suffix-match without a
/// low-confidence warning. A bare basename (`mod.rs`, depth 1) warns;
/// `src/lib.rs` (depth 2) does not.
pub const FILE_ATTENTION_MIN_TAIL_SEGMENTS: usize = 2;

pub type ContractResult<T> = Result<T, ContractError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpEntityKind {
    Session,
    Turn,
    Event,
}

impl McpEntityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Turn => "turn",
            Self::Event => "event",
        }
    }
}

impl fmt::Display for McpEntityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpSessionId {
    raw_session_id: String,
}

impl McpSessionId {
    pub fn from_raw_session_id(raw_session_id: impl Into<String>) -> ContractResult<Self> {
        let raw_session_id = raw_session_id.into();
        validate_raw_id("session_id", &raw_session_id)?;
        Ok(Self { raw_session_id })
    }

    pub fn raw_session_id(&self) -> &str {
        &self.raw_session_id
    }

    pub fn kind(&self) -> McpEntityKind {
        McpEntityKind::Session
    }
}

impl fmt::Display for McpSessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{SESSION_ID_PREFIX}{}",
            encode_base64_url_no_pad(self.raw_session_id.as_bytes())
        )
    }
}

impl FromStr for McpSessionId {
    type Err = ContractError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let encoded = input
            .strip_prefix(SESSION_ID_PREFIX)
            .ok_or_else(|| invalid_id("expected session ID"))?;
        let raw_session_id = decode_raw_id("session_id", encoded)?;
        Self::from_raw_session_id(raw_session_id)
    }
}

impl Serialize for McpSessionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for McpSessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        input.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpEventId {
    raw_event_uid: String,
}

impl McpEventId {
    pub fn from_raw_event_uid(raw_event_uid: impl Into<String>) -> ContractResult<Self> {
        let raw_event_uid = raw_event_uid.into();
        validate_raw_id("event_uid", &raw_event_uid)?;
        Ok(Self { raw_event_uid })
    }

    pub fn raw_event_uid(&self) -> &str {
        &self.raw_event_uid
    }

    pub fn kind(&self) -> McpEntityKind {
        McpEntityKind::Event
    }
}

impl fmt::Display for McpEventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{EVENT_ID_PREFIX}{}",
            encode_base64_url_no_pad(self.raw_event_uid.as_bytes())
        )
    }
}

impl FromStr for McpEventId {
    type Err = ContractError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let encoded = input
            .strip_prefix(EVENT_ID_PREFIX)
            .ok_or_else(|| invalid_id("expected event ID"))?;
        let raw_event_uid = decode_raw_id("event_uid", encoded)?;
        Self::from_raw_event_uid(raw_event_uid)
    }
}

impl Serialize for McpEventId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for McpEventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        input.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpTurnId {
    raw_session_id: String,
    turn_seq: u32,
}

impl McpTurnId {
    pub fn from_raw_session_id_and_turn_seq(
        raw_session_id: impl Into<String>,
        turn_seq: u32,
    ) -> ContractResult<Self> {
        let raw_session_id = raw_session_id.into();
        validate_raw_id("session_id", &raw_session_id)?;
        Ok(Self {
            raw_session_id,
            turn_seq,
        })
    }

    pub fn raw_session_id(&self) -> &str {
        &self.raw_session_id
    }

    pub fn turn_seq(&self) -> u32 {
        self.turn_seq
    }

    pub fn decode(&self) -> (&str, u32) {
        (&self.raw_session_id, self.turn_seq)
    }

    pub fn kind(&self) -> McpEntityKind {
        McpEntityKind::Turn
    }
}

impl fmt::Display for McpTurnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{TURN_ID_PREFIX}{}:{}",
            encode_base64_url_no_pad(self.raw_session_id.as_bytes()),
            self.turn_seq
        )
    }
}

impl FromStr for McpTurnId {
    type Err = ContractError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let payload = input
            .strip_prefix(TURN_ID_PREFIX)
            .ok_or_else(|| invalid_id("expected turn ID"))?;
        let (encoded_session_id, turn_seq) = payload
            .rsplit_once(':')
            .ok_or_else(|| invalid_id("turn ID must contain session and turn sequence"))?;
        let raw_session_id = decode_raw_id("session_id", encoded_session_id)?;
        let turn_seq = turn_seq
            .parse::<u32>()
            .map_err(|_| invalid_id("turn sequence must be an unsigned integer"))?;
        Self::from_raw_session_id_and_turn_seq(raw_session_id, turn_seq)
    }
}

impl Serialize for McpTurnId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for McpTurnId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        input.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum McpId {
    Session(McpSessionId),
    Turn(McpTurnId),
    Event(McpEventId),
}

impl McpId {
    pub fn kind(&self) -> McpEntityKind {
        match self {
            Self::Session(id) => id.kind(),
            Self::Turn(id) => id.kind(),
            Self::Event(id) => id.kind(),
        }
    }

    pub fn as_session(&self) -> Option<&McpSessionId> {
        match self {
            Self::Session(id) => Some(id),
            _ => None,
        }
    }

    pub fn as_turn(&self) -> Option<&McpTurnId> {
        match self {
            Self::Turn(id) => Some(id),
            _ => None,
        }
    }

    pub fn as_event(&self) -> Option<&McpEventId> {
        match self {
            Self::Event(id) => Some(id),
            _ => None,
        }
    }
}

impl fmt::Display for McpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(id) => id.fmt(f),
            Self::Turn(id) => id.fmt(f),
            Self::Event(id) => id.fmt(f),
        }
    }
}

impl FromStr for McpId {
    type Err = ContractError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.starts_with(SESSION_ID_PREFIX) {
            Ok(Self::Session(input.parse()?))
        } else if input.starts_with(TURN_ID_PREFIX) {
            Ok(Self::Turn(input.parse()?))
        } else if input.starts_with(EVENT_ID_PREFIX) {
            Ok(Self::Event(input.parse()?))
        } else {
            Err(invalid_id("ID must begin with session:, turn:, or event:"))
        }
    }
}

impl Serialize for McpId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for McpId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        input.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpEventType {
    UserInput,
    AssistantResponse,
    Reasoning,
    ToolCall,
    ToolResponse,
    Compaction,
    System,
    Runtime,
    Unknown,
}

impl McpEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserInput => "user_input",
            Self::AssistantResponse => "assistant_response",
            Self::Reasoning => "reasoning",
            Self::ToolCall => "tool_call",
            Self::ToolResponse => "tool_response",
            Self::Compaction => "compaction",
            Self::System => "system",
            Self::Runtime => "runtime",
            Self::Unknown => "unknown",
        }
    }

    pub fn all() -> &'static [McpEventType] {
        &[
            Self::UserInput,
            Self::AssistantResponse,
            Self::Reasoning,
            Self::ToolCall,
            Self::ToolResponse,
            Self::Compaction,
            Self::System,
            Self::Runtime,
            Self::Unknown,
        ]
    }

    pub fn searchable() -> &'static [McpEventType] {
        &[
            Self::UserInput,
            Self::AssistantResponse,
            Self::Reasoning,
            Self::ToolCall,
            Self::ToolResponse,
            Self::Compaction,
            Self::System,
            Self::Runtime,
        ]
    }

    pub fn search_defaults() -> &'static [McpEventType] {
        &[Self::UserInput, Self::AssistantResponse, Self::ToolResponse]
    }

    pub fn is_searchable(self) -> bool {
        Self::searchable().contains(&self)
    }

    pub fn name_list(event_types: &[McpEventType]) -> Vec<&'static str> {
        event_types
            .iter()
            .map(|event_type| event_type.as_str())
            .collect()
    }

    fn from_name(input: &str) -> Option<Self> {
        match input {
            "user_input" => Some(Self::UserInput),
            "assistant_response" => Some(Self::AssistantResponse),
            "reasoning" => Some(Self::Reasoning),
            "tool_call" => Some(Self::ToolCall),
            "tool_response" => Some(Self::ToolResponse),
            "compaction" => Some(Self::Compaction),
            "system" => Some(Self::System),
            "runtime" => Some(Self::Runtime),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

impl fmt::Display for McpEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for McpEventType {
    type Err = ContractError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::from_name(input).ok_or_else(|| {
            unsupported_event_type(input, McpEventType::name_list(McpEventType::all()))
        })
    }
}

pub fn default_search_event_types() -> Vec<McpEventType> {
    McpEventType::search_defaults().to_vec()
}

pub fn canonicalize_search_event_type_names<T: AsRef<str>>(
    event_types: &[T],
) -> ContractResult<Vec<McpEventType>> {
    if event_types.is_empty() {
        return Err(invalid_request_with_field(
            "event_types",
            "event_types must contain at least one event type",
        ));
    }

    let mut selected = [false; 8];
    for raw in event_types {
        let raw = raw.as_ref();
        let event_type = McpEventType::from_name(raw).ok_or_else(|| {
            unsupported_event_type(raw, McpEventType::name_list(McpEventType::searchable()))
        })?;
        let Some(index) = searchable_event_type_index(event_type) else {
            return Err(unsupported_event_type(
                raw,
                McpEventType::name_list(McpEventType::searchable()),
            ));
        };
        selected[index] = true;
    }

    let canonical = McpEventType::searchable()
        .iter()
        .enumerate()
        .filter_map(|(index, event_type)| selected[index].then_some(*event_type))
        .collect::<Vec<_>>();

    if canonical.is_empty() {
        return Err(invalid_request_with_field(
            "event_types",
            "event_types must contain at least one event type",
        ));
    }

    Ok(canonical)
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchSessionsArgs {
    pub query: String,
    #[serde(default)]
    pub within_id: Option<String>,
    #[serde(default)]
    pub event_types: Option<Vec<String>>,
    #[serde(default)]
    pub n_hits: Option<i64>,
}

impl SearchSessionsArgs {
    pub fn validate(self) -> ContractResult<CanonicalSearchSessionsArgs> {
        let query = self.query.trim().to_string();
        if query.is_empty() {
            return Err(invalid_request_with_field(
                "query",
                "query must be a non-empty string",
            ));
        }
        if query.chars().count() > SEARCH_SESSIONS_MAX_QUERY_CHARS {
            return Err(invalid_request_with_field(
                "query",
                "query must be at most 4096 characters",
            ));
        }

        let within_id = self
            .within_id
            .map(|within_id| within_id.parse::<McpId>())
            .transpose()?;
        if matches!(within_id, Some(McpId::Event(_))) {
            return Err(invalid_request_with_field(
                "within_id",
                "within_id accepts session and turn IDs, not event IDs",
            ));
        }

        let event_types = match self.event_types {
            Some(event_types) => canonicalize_search_event_type_names(&event_types)?,
            None => default_search_event_types(),
        };

        let n_hits = self
            .n_hits
            .unwrap_or(i64::from(SEARCH_SESSIONS_DEFAULT_N_HITS));
        if !(i64::from(SEARCH_SESSIONS_MIN_N_HITS)..=i64::from(SEARCH_SESSIONS_MAX_N_HITS))
            .contains(&n_hits)
        {
            return Err(invalid_request_with_field(
                "n_hits",
                "n_hits must be between 1 and 50",
            ));
        }
        let n_hits = n_hits as u16;

        Ok(CanonicalSearchSessionsArgs {
            query,
            within_id,
            event_types,
            n_hits,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalSearchSessionsArgs {
    pub query: String,
    pub within_id: Option<McpId>,
    pub event_types: Vec<McpEventType>,
    pub n_hits: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListSessionsMode {
    WebSearch,
    McpInternal,
    ToolCalling,
    Chat,
}

impl ListSessionsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WebSearch => "web_search",
            Self::McpInternal => "mcp_internal",
            Self::ToolCalling => "tool_calling",
            Self::Chat => "chat",
        }
    }
}

impl fmt::Display for ListSessionsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListSessionsSort {
    Asc,
    #[default]
    Desc,
}

impl ListSessionsSort {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

impl fmt::Display for ListSessionsSort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSessionsArgs {
    #[serde(default)]
    pub start_datetime: Option<String>,
    #[serde(default)]
    pub end_datetime: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub mode: Option<ListSessionsMode>,
    #[serde(default)]
    pub sort: Option<ListSessionsSort>,
}

impl ListSessionsArgs {
    pub fn validate(self, max_results: u16) -> ContractResult<CanonicalListSessionsArgs> {
        let max_limit = max_results.max(LIST_SESSIONS_MIN_LIMIT);
        let limit = self
            .limit
            .unwrap_or(i64::from(LIST_SESSIONS_DEFAULT_LIMIT.min(max_limit)));
        if !(i64::from(LIST_SESSIONS_MIN_LIMIT)..=i64::from(max_limit)).contains(&limit) {
            return Err(invalid_request_with_field(
                "limit",
                format!("limit must be between 1 and {max_limit}"),
            ));
        }
        let limit = limit as u16;

        let Some(start_datetime) = self.start_datetime else {
            return Err(list_sessions_datetime_required_error());
        };
        let Some(end_datetime) = self.end_datetime else {
            return Err(list_sessions_datetime_required_error());
        };

        let start_datetime = start_datetime.trim().to_string();
        let end_datetime = end_datetime.trim().to_string();
        let start_unix_ms = parse_explicit_timezone_datetime("start_datetime", &start_datetime)?;
        let end_unix_ms = parse_explicit_timezone_datetime("end_datetime", &end_datetime)?;
        if end_unix_ms <= start_unix_ms {
            return Err(invalid_request_with_field(
                "end_datetime",
                "end_datetime must be later than start_datetime",
            ));
        }

        let cursor = self.cursor.map(|cursor| cursor.trim().to_string());
        if matches!(cursor.as_deref(), Some("")) {
            return Err(invalid_request_with_field(
                "cursor",
                "cursor must be a non-empty string when provided",
            ));
        }

        Ok(CanonicalListSessionsArgs {
            start_datetime,
            end_datetime,
            start_unix_ms,
            end_unix_ms,
            limit,
            cursor,
            mode: self.mode,
            sort: self.sort.unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalListSessionsArgs {
    pub start_datetime: String,
    pub end_datetime: String,
    pub start_unix_ms: i64,
    pub end_unix_ms: i64,
    pub limit: u16,
    pub cursor: Option<String>,
    pub mode: Option<ListSessionsMode>,
    pub sort: ListSessionsSort,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct OpenV1Args {
    #[serde(default)]
    pub id: Option<String>,
}

impl OpenV1Args {
    pub fn validate(self) -> ContractResult<CanonicalOpenV1Args> {
        let Some(id) = self.id else {
            return Err(invalid_request_with_field("id", "id is required"));
        };
        if id.trim().is_empty() {
            return Err(invalid_request_with_field(
                "id",
                "id must be a non-empty string",
            ));
        }

        Ok(CanonicalOpenV1Args { id: id.parse()? })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalOpenV1Args {
    pub id: McpId,
}

/// How far `file_attention` widens its search.
///
/// `Project` (default) keeps the answer focused on the normalized identity of
/// the launch project, independently of `--project-only`. `All` is the
/// deliberate widen for unscoped servers: it drops request-level project
/// narrowing so a touch in any project the backend holds can be returned. A
/// configured server scope remains a hard floor, so `scope` never exposes IDs
/// that `open` would reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileAttentionScope {
    #[default]
    Project,
    All,
}

impl FileAttentionScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::All => "all",
        }
    }
}

impl fmt::Display for FileAttentionScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Shape of the `file_attention` result body.
///
/// `Sessions` (default) returns one rollup per session that touched the file;
/// `Events` returns the flat, time-ordered touch-by-touch timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileAttentionGranularity {
    #[default]
    Sessions,
    Events,
}

impl FileAttentionGranularity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Events => "events",
        }
    }
}

impl fmt::Display for FileAttentionGranularity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileAttentionArgs {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub scope: Option<FileAttentionScope>,
    #[serde(default)]
    pub granularity: Option<FileAttentionGranularity>,
    #[serde(default)]
    pub start_datetime: Option<String>,
    #[serde(default)]
    pub end_datetime: Option<String>,
    #[serde(default)]
    pub tool: Option<String>,
    #[serde(default)]
    pub mutations_only: Option<bool>,
    #[serde(default)]
    pub limit: Option<i64>,
}

impl FileAttentionArgs {
    pub fn validate(self, max_results: u16) -> ContractResult<CanonicalFileAttentionArgs> {
        let Some(path) = self.path else {
            return Err(invalid_request_with_field("path", "path is required"));
        };
        if path != path.trim() {
            return Err(invalid_request_with_field(
                "path",
                "path must not have leading or trailing whitespace",
            ));
        }
        if path.starts_with("file://") {
            return Err(invalid_request_with_field(
                "path",
                "file:// URIs are not supported; pass a filesystem path",
            ));
        }
        if path.ends_with('/') {
            return Err(invalid_request_with_field(
                "path",
                "path must name a file, not a directory-style path ending in '/'",
            ));
        }
        if path.is_empty() {
            return Err(invalid_request_with_field(
                "path",
                "path must be a non-empty string",
            ));
        }

        let max_limit = max_results.max(FILE_ATTENTION_MIN_LIMIT);
        let limit = self
            .limit
            .unwrap_or(i64::from(FILE_ATTENTION_DEFAULT_LIMIT.min(max_limit)));
        if !(i64::from(FILE_ATTENTION_MIN_LIMIT)..=i64::from(max_limit)).contains(&limit) {
            return Err(invalid_request_with_field(
                "limit",
                format!("limit must be between 1 and {max_limit}"),
            ));
        }
        let limit = limit as u16;

        // Both bounds are optional and independent, but if both are supplied
        // the window must be non-empty — same contract as list_sessions.
        let start = parse_optional_datetime("start_datetime", self.start_datetime)?;
        let end = parse_optional_datetime("end_datetime", self.end_datetime)?;
        if let (Some((start_ms, _)), Some((end_ms, _))) = (&start, &end) {
            if end_ms <= start_ms {
                return Err(invalid_request_with_field(
                    "end_datetime",
                    "end_datetime must be later than start_datetime",
                ));
            }
        }

        let tool = self
            .tool
            .map(|tool| tool.trim().to_string())
            .filter(|tool| !tool.is_empty());

        Ok(CanonicalFileAttentionArgs {
            path,
            scope: self.scope.unwrap_or_default(),
            granularity: self.granularity.unwrap_or_default(),
            start_datetime: start.as_ref().map(|(_, raw)| raw.clone()),
            end_datetime: end.as_ref().map(|(_, raw)| raw.clone()),
            start_unix_ms: start.map(|(ms, _)| ms),
            end_unix_ms: end.map(|(ms, _)| ms),
            tool,
            mutations_only: self.mutations_only.unwrap_or(false),
            limit,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalFileAttentionArgs {
    pub path: String,
    pub scope: FileAttentionScope,
    pub granularity: FileAttentionGranularity,
    pub start_datetime: Option<String>,
    pub end_datetime: Option<String>,
    pub start_unix_ms: Option<i64>,
    pub end_unix_ms: Option<i64>,
    pub tool: Option<String>,
    pub mutations_only: bool,
    pub limit: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorCode {
    InvalidRequest,
    InvalidId,
    NotFound,
    UnsupportedEventType,
    DeadlineExceeded,
    InternalError,
}

impl ToolErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidId => "invalid_id",
            Self::NotFound => "not_found",
            Self::UnsupportedEventType => "unsupported_event_type",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::InternalError => "internal_error",
        }
    }
}

impl fmt::Display for ToolErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContractError {
    code: ToolErrorCode,
    message: String,
    details: Option<Value>,
}

impl ContractError {
    pub fn new(code: ToolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn code(&self) -> ToolErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn details(&self) -> Option<&Value> {
        self.details.as_ref()
    }
}

impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ContractError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolError {
    pub code: ToolErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl From<ContractError> for ToolError {
    fn from(error: ContractError) -> Self {
        Self {
            code: error.code,
            message: error.message,
            details: error.details,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Performance {
    pub elapsed_ms: u64,
    pub sla_target_ms: u64,
    pub met_sla: bool,
}

impl Performance {
    pub fn from_elapsed_ms(elapsed_ms: u64, sla_target_ms: u64) -> Self {
        Self {
            elapsed_ms,
            sla_target_ms,
            met_sla: elapsed_ms <= sla_target_ms,
        }
    }

    pub fn from_elapsed(elapsed: Duration, sla_target_ms: u64) -> Self {
        Self::from_elapsed_ms(duration_millis_u64(elapsed), sla_target_ms)
    }

    pub fn builder(sla_target_ms: u64) -> PerformanceBuilder {
        PerformanceBuilder::new(sla_target_ms)
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceBuilder {
    started_at: Instant,
    sla_target_ms: u64,
}

impl PerformanceBuilder {
    pub fn new(sla_target_ms: u64) -> Self {
        Self {
            started_at: Instant::now(),
            sla_target_ms,
        }
    }

    pub fn finish(&self) -> Performance {
        Performance::from_elapsed(self.started_at.elapsed(), self.sla_target_ms)
    }

    pub fn with_sla_target(&self, sla_target_ms: u64) -> Self {
        Self {
            started_at: self.started_at,
            sla_target_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolEnvelope<TRequest, TData> {
    pub schema_version: String,
    pub tool: String,
    pub request: TRequest,
    pub data: TData,
    pub warnings: Vec<String>,
    pub performance: Performance,
}

impl<TRequest, TData> ToolEnvelope<TRequest, TData> {
    pub fn success(
        tool: impl Into<String>,
        request: TRequest,
        data: TData,
        performance: Performance,
    ) -> Self {
        let tool = tool.into();
        Self {
            schema_version: tool_schema_version(&tool),
            tool,
            request,
            data,
            warnings: Vec::new(),
            performance,
        }
    }

    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }

    pub fn with_warnings<I, S>(mut self, warnings: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.warnings.extend(warnings.into_iter().map(Into::into));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolErrorEnvelope<TRequest> {
    pub schema_version: String,
    pub tool: String,
    pub request: TRequest,
    pub error: ToolError,
    pub warnings: Vec<String>,
    pub performance: Performance,
}

impl<TRequest> ToolErrorEnvelope<TRequest> {
    pub fn error(
        tool: impl Into<String>,
        request: TRequest,
        error: impl Into<ToolError>,
        performance: Performance,
    ) -> Self {
        Self {
            schema_version: ERROR_SCHEMA_VERSION.to_string(),
            tool: tool.into(),
            request,
            error: error.into(),
            warnings: Vec::new(),
            performance,
        }
    }

    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

pub fn tool_schema_version(tool: &str) -> String {
    format!("moraine.mcp.{tool}.v1")
}

pub fn format_rfc3339_utc_millis(unix_ms: i64) -> String {
    let seconds = unix_ms.div_euclid(1000);
    let millis = unix_ms.rem_euclid(1000);
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

fn validate_raw_id(field: &'static str, raw_id: &str) -> ContractResult<()> {
    if raw_id.trim().is_empty() {
        return Err(invalid_id(format!("{field} must be a non-empty string")));
    }
    Ok(())
}

fn decode_raw_id(field: &'static str, encoded: &str) -> ContractResult<String> {
    let bytes = decode_base64_url_no_pad(encoded)
        .map_err(|_| invalid_id(format!("{field} is not valid URL-safe base64")))?;
    let raw_id =
        String::from_utf8(bytes).map_err(|_| invalid_id(format!("{field} is not valid UTF-8")))?;
    validate_raw_id(field, &raw_id)?;
    Ok(raw_id)
}

fn invalid_id(message: impl Into<String>) -> ContractError {
    ContractError::new(ToolErrorCode::InvalidId, message)
}

fn invalid_request_with_field(field: &'static str, message: impl Into<String>) -> ContractError {
    ContractError::new(ToolErrorCode::InvalidRequest, message)
        .with_details(json!({ "field": field }))
}

fn unsupported_event_type(raw: &str, supported: Vec<&'static str>) -> ContractError {
    ContractError::new(
        ToolErrorCode::UnsupportedEventType,
        format!("unsupported event type: {raw}"),
    )
    .with_details(json!({
        "field": "event_types",
        "supported": supported
    }))
}

fn list_sessions_datetime_required_error() -> ContractError {
    ContractError::new(
        ToolErrorCode::InvalidRequest,
        "list_sessions requires start_datetime and end_datetime with explicit timezones",
    )
    .with_details(json!({
        "example": {
            "start_datetime": "2026-04-30T09:00:00-04:00",
            "end_datetime": "2026-04-30T13:00:00-04:00"
        }
    }))
}

/// Parse an optional datetime bound, returning `(unix_ms, trimmed_input)`.
/// Absent or blank input yields `None`; present input must carry an explicit
/// timezone, matching `parse_explicit_timezone_datetime`.
fn parse_optional_datetime(
    field: &'static str,
    input: Option<String>,
) -> ContractResult<Option<(i64, String)>> {
    let Some(raw) = input else {
        return Ok(None);
    };
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_millisecond_precision(field, &trimmed)?;
    let unix_ms = parse_explicit_timezone_datetime(field, &trimmed)?;
    Ok(Some((unix_ms, trimmed)))
}

fn validate_millisecond_precision(field: &'static str, input: &str) -> ContractResult<()> {
    let Some(time_start) = input.find('T').or_else(|| input.find('t')) else {
        return Ok(());
    };
    let time = &input[time_start + 1..];
    let Some(dot) = time.find('.') else {
        return Ok(());
    };
    let fraction = time[dot + 1..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .count();
    if fraction > 3 {
        return Err(invalid_request_with_field(
            field,
            format!("{field} supports millisecond precision at most"),
        ));
    }
    Ok(())
}

fn parse_explicit_timezone_datetime(field: &'static str, input: &str) -> ContractResult<i64> {
    if input.is_empty() {
        return Err(invalid_request_with_field(
            field,
            format!("{field} must be a non-empty RFC 3339 datetime"),
        ));
    }
    if !has_explicit_timezone(input) {
        return Err(invalid_request_with_field(
            field,
            format!("{field} must include an explicit timezone offset or Z"),
        ));
    }

    DateTime::parse_from_rfc3339(input)
        .map(|datetime| datetime.timestamp_millis())
        .map_err(|error| {
            invalid_request_with_field(
                field,
                format!("{field} must be a valid RFC 3339 datetime: {error}"),
            )
        })
}

fn has_explicit_timezone(input: &str) -> bool {
    let input = input.trim();
    if input.ends_with('Z') || input.ends_with('z') {
        return true;
    }

    let bytes = input.as_bytes();
    if bytes.len() < 6 {
        return false;
    }
    let offset = &bytes[bytes.len() - 6..];
    matches!(offset[0], b'+' | b'-')
        && offset[1].is_ascii_digit()
        && offset[2].is_ascii_digit()
        && offset[3] == b':'
        && offset[4].is_ascii_digit()
        && offset[5].is_ascii_digit()
}

fn searchable_event_type_index(event_type: McpEventType) -> Option<usize> {
    McpEventType::searchable()
        .iter()
        .position(|candidate| *candidate == event_type)
}

fn encode_base64_url_no_pad(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut chunks = bytes.chunks_exact(3);

    for chunk in &mut chunks {
        let value = ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8) | chunk[2] as u32;
        output.push(BASE64_URL_ALPHABET[((value >> 18) & 0x3f) as usize] as char);
        output.push(BASE64_URL_ALPHABET[((value >> 12) & 0x3f) as usize] as char);
        output.push(BASE64_URL_ALPHABET[((value >> 6) & 0x3f) as usize] as char);
        output.push(BASE64_URL_ALPHABET[(value & 0x3f) as usize] as char);
    }

    match chunks.remainder() {
        [one] => {
            let value = (*one as u32) << 16;
            output.push(BASE64_URL_ALPHABET[((value >> 18) & 0x3f) as usize] as char);
            output.push(BASE64_URL_ALPHABET[((value >> 12) & 0x3f) as usize] as char);
        }
        [one, two] => {
            let value = ((*one as u32) << 16) | ((*two as u32) << 8);
            output.push(BASE64_URL_ALPHABET[((value >> 18) & 0x3f) as usize] as char);
            output.push(BASE64_URL_ALPHABET[((value >> 12) & 0x3f) as usize] as char);
            output.push(BASE64_URL_ALPHABET[((value >> 6) & 0x3f) as usize] as char);
        }
        [] => {}
        _ => unreachable!("chunks_exact(3) remainder has at most two bytes"),
    }

    output
}

fn decode_base64_url_no_pad(input: &str) -> Result<Vec<u8>, ()> {
    if input.is_empty() || input.len() % 4 == 1 || input.contains('=') {
        return Err(());
    }

    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = 0_u32;
    let mut bits = 0_u8;

    for byte in input.bytes() {
        let value = decode_base64_url_byte(byte).ok_or(())? as u32;
        buffer = (buffer << 6) | value;
        bits += 6;

        while bits >= 8 {
            bits -= 8;
            output.push((buffer >> bits) as u8);
            buffer &= (1_u32 << bits) - 1;
        }
    }

    if bits > 0 && buffer != 0 {
        return Err(());
    }

    Ok(output)
}

fn decode_base64_url_byte(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };

    (year as i32, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mcp_id_round_trips_session_event_and_turn() {
        let session = McpSessionId::from_raw_session_id("sess-1").expect("session id");
        let event = McpEventId::from_raw_event_uid("evt-1").expect("event id");
        let turn = McpTurnId::from_raw_session_id_and_turn_seq("sess-1", 42).expect("turn id");

        assert_eq!(session.to_string(), "session:c2Vzcy0x");
        assert_eq!(event.to_string(), "event:ZXZ0LTE");
        assert_eq!(turn.to_string(), "turn:c2Vzcy0x:42");

        let parsed_session: McpId = session.to_string().parse().expect("parse session");
        let parsed_event: McpId = event.to_string().parse().expect("parse event");
        let parsed_turn: McpId = turn.to_string().parse().expect("parse turn");

        assert_eq!(parsed_session.kind(), McpEntityKind::Session);
        assert_eq!(
            parsed_session
                .as_session()
                .expect("session")
                .raw_session_id(),
            "sess-1"
        );
        assert_eq!(parsed_event.kind(), McpEntityKind::Event);
        assert_eq!(
            parsed_event.as_event().expect("event").raw_event_uid(),
            "evt-1"
        );

        let parsed_turn = parsed_turn.as_turn().expect("turn");
        assert_eq!(parsed_turn.decode(), ("sess-1", 42));
    }

    #[test]
    fn mcp_id_encoding_is_url_safe_and_reversible_for_raw_ids() {
        let raw_session_id = "sess/with spaces?and=equals+plus:colon";
        let raw_event_uid = "evt/with spaces?and=equals+plus:colon";
        let session = McpSessionId::from_raw_session_id(raw_session_id).expect("session id");
        let event = McpEventId::from_raw_event_uid(raw_event_uid).expect("event id");

        for id in [session.to_string(), event.to_string()] {
            assert!(id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ':' | '-' | '_')));
        }

        assert_eq!(
            session
                .to_string()
                .parse::<McpSessionId>()
                .expect("parse session")
                .raw_session_id(),
            raw_session_id
        );
        assert_eq!(
            event
                .to_string()
                .parse::<McpEventId>()
                .expect("parse event")
                .raw_event_uid(),
            raw_event_uid
        );
    }

    #[test]
    fn mcp_id_rejects_malformed_and_blank_ids() {
        for input in [
            "",
            " ",
            "session:",
            "session:!!!!",
            "session:A",
            "turn:c2Vzcy0x",
            "turn:c2Vzcy0x:",
            "turn:c2Vzcy0x:not-a-number",
            "event:",
            "unknown:c2Vzcy0x",
        ] {
            let error = input.parse::<McpId>().expect_err("invalid id");
            assert_eq!(error.code(), ToolErrorCode::InvalidId, "{input}");
        }

        assert_eq!(
            McpSessionId::from_raw_session_id("")
                .expect_err("blank raw session")
                .code(),
            ToolErrorCode::InvalidId
        );
        assert_eq!(
            McpEventId::from_raw_event_uid(" ")
                .expect_err("blank raw event")
                .code(),
            ToolErrorCode::InvalidId
        );
        assert_eq!(
            McpTurnId::from_raw_session_id_and_turn_seq("", 1)
                .expect_err("blank raw turn")
                .code(),
            ToolErrorCode::InvalidId
        );
    }

    #[test]
    fn event_type_parse_and_serialize() {
        assert_eq!(
            "tool_call".parse::<McpEventType>().expect("event type"),
            McpEventType::ToolCall
        );
        assert_eq!(McpEventType::ToolCall.to_string(), "tool_call");
        assert_eq!(
            serde_json::to_value(McpEventType::AssistantResponse).expect("serialize"),
            json!("assistant_response")
        );
    }

    #[test]
    fn event_type_rejects_unsupported_type() {
        let error = "debug_trace"
            .parse::<McpEventType>()
            .expect_err("unsupported event type");
        assert_eq!(error.code(), ToolErrorCode::UnsupportedEventType);
        assert!(error.message().contains("debug_trace"));
    }

    #[test]
    fn event_type_filter_dedupes_in_canonical_order() {
        let event_types = canonicalize_search_event_type_names(&[
            "tool_response",
            "user_input",
            "assistant_response",
            "user_input",
            "tool_call",
        ])
        .expect("canonical event types");

        assert_eq!(
            event_types,
            vec![
                McpEventType::UserInput,
                McpEventType::AssistantResponse,
                McpEventType::ToolCall,
                McpEventType::ToolResponse,
            ]
        );
    }

    #[test]
    fn event_type_filter_rejects_unsupported_and_unknown() {
        let unsupported = canonicalize_search_event_type_names(&["user_input", "debug_trace"])
            .expect_err("unsupported");
        assert_eq!(unsupported.code(), ToolErrorCode::UnsupportedEventType);
        assert_eq!(
            unsupported.details().expect("details")["supported"],
            json!([
                "user_input",
                "assistant_response",
                "reasoning",
                "tool_call",
                "tool_response",
                "compaction",
                "system",
                "runtime"
            ])
        );

        let unknown = canonicalize_search_event_type_names(&["unknown"]).expect_err("unknown");
        assert_eq!(unknown.code(), ToolErrorCode::UnsupportedEventType);
    }

    #[test]
    fn default_event_types_are_user_assistant_and_tool_response() {
        assert_eq!(
            default_search_event_types(),
            vec![
                McpEventType::UserInput,
                McpEventType::AssistantResponse,
                McpEventType::ToolResponse
            ]
        );

        let canonical = SearchSessionsArgs {
            query: " migration ".to_string(),
            within_id: None,
            event_types: None,
            n_hits: None,
        }
        .validate()
        .expect("valid args");

        assert_eq!(canonical.query, "migration");
        assert_eq!(canonical.event_types, default_search_event_types());
        assert_eq!(canonical.n_hits, SEARCH_SESSIONS_DEFAULT_N_HITS);
    }

    #[test]
    fn search_sessions_args_validate_scope_and_limits() {
        let session_id = McpSessionId::from_raw_session_id("sess-1")
            .expect("session")
            .to_string();
        let args = SearchSessionsArgs {
            query: "query".to_string(),
            within_id: Some(session_id.clone()),
            event_types: Some(vec!["tool_response".to_string(), "user_input".to_string()]),
            n_hits: Some(25),
        };

        let canonical = args.validate().expect("valid args");
        assert_eq!(canonical.within_id.expect("within").to_string(), session_id);
        assert_eq!(
            canonical.event_types,
            vec![McpEventType::UserInput, McpEventType::ToolResponse]
        );
        assert_eq!(canonical.n_hits, 25);

        let blank_query = SearchSessionsArgs {
            query: " ".to_string(),
            within_id: None,
            event_types: None,
            n_hits: None,
        }
        .validate()
        .expect_err("blank query");
        assert_eq!(blank_query.code(), ToolErrorCode::InvalidRequest);

        let event_id = McpEventId::from_raw_event_uid("evt-1")
            .expect("event")
            .to_string();
        let event_scope = SearchSessionsArgs {
            query: "query".to_string(),
            within_id: Some(event_id),
            event_types: None,
            n_hits: None,
        }
        .validate()
        .expect_err("event scope");
        assert_eq!(event_scope.code(), ToolErrorCode::InvalidRequest);
    }

    #[test]
    fn file_attention_args_reject_path_hygiene_issues() {
        for path in [
            " crates/moraine-mcp-core/src/file_attention_v1.rs",
            "crates/moraine-mcp-core/src/file_attention_v1.rs ",
            "file:///Users/me/src/moraine/Cargo.toml",
            "crates/moraine-mcp-core/src/",
        ] {
            let error = FileAttentionArgs {
                path: Some(path.to_string()),
                ..FileAttentionArgs::default()
            }
            .validate(25)
            .expect_err("path should be rejected");
            assert_eq!(error.code(), ToolErrorCode::InvalidRequest);
            assert_eq!(error.details().expect("details")["field"], json!("path"));
        }
    }

    #[test]
    fn file_attention_args_reject_sub_millisecond_bounds() {
        let error = FileAttentionArgs {
            path: Some("Cargo.toml".to_string()),
            start_datetime: Some("2026-06-16T07:04:22.918999999Z".to_string()),
            end_datetime: Some("2026-06-16T07:04:22.919Z".to_string()),
            ..FileAttentionArgs::default()
        }
        .validate(25)
        .expect_err("sub-ms start should be rejected");
        assert_eq!(error.code(), ToolErrorCode::InvalidRequest);
        assert_eq!(
            error.details().expect("details")["field"],
            json!("start_datetime")
        );
    }

    #[test]
    fn open_v1_args_validate_id_presence_and_shape() {
        let id = McpTurnId::from_raw_session_id_and_turn_seq("sess-1", 7)
            .expect("turn")
            .to_string();
        let canonical = OpenV1Args {
            id: Some(id.clone()),
        }
        .validate()
        .expect("valid open args");
        assert_eq!(canonical.id.to_string(), id);

        let missing = OpenV1Args { id: None }.validate().expect_err("missing id");
        assert_eq!(missing.code(), ToolErrorCode::InvalidRequest);

        let blank = OpenV1Args {
            id: Some(" ".to_string()),
        }
        .validate()
        .expect_err("blank id");
        assert_eq!(blank.code(), ToolErrorCode::InvalidRequest);

        let malformed = OpenV1Args {
            id: Some("session:!!!!".to_string()),
        }
        .validate()
        .expect_err("malformed id");
        assert_eq!(malformed.code(), ToolErrorCode::InvalidId);
    }

    #[test]
    fn success_and_error_envelopes_have_contract_shape() {
        let performance = Performance::from_elapsed_ms(64, SEARCH_SESSIONS_SLA_TARGET_MS);
        let success = ToolEnvelope::success(
            SEARCH_SESSIONS_TOOL,
            json!({
                "query": "migration",
                "within_id": null,
                "event_types": ["user_input", "assistant_response", "tool_response"],
                "n_hits": 10
            }),
            json!({
                "result_count": 0,
                "limit": 10,
                "truncated": false,
                "results": []
            }),
            performance,
        );

        assert_eq!(
            serde_json::to_value(success).expect("serialize success"),
            json!({
                "schema_version": SEARCH_SESSIONS_SCHEMA_VERSION,
                "tool": SEARCH_SESSIONS_TOOL,
                "request": {
                    "query": "migration",
                    "within_id": null,
                    "event_types": ["user_input", "assistant_response", "tool_response"],
                    "n_hits": 10
                },
                "data": {
                    "result_count": 0,
                    "limit": 10,
                    "truncated": false,
                    "results": []
                },
                "warnings": [],
                "performance": {
                    "elapsed_ms": 64,
                    "sla_target_ms": 750,
                    "met_sla": true
                }
            })
        );

        let error = ContractError::new(
            ToolErrorCode::InvalidRequest,
            "query must be a non-empty string",
        )
        .with_details(json!({ "field": "query" }));
        let error_envelope = ToolErrorEnvelope::error(
            SEARCH_SESSIONS_TOOL,
            json!({ "query": "   " }),
            error,
            Performance::from_elapsed_ms(2, SEARCH_SESSIONS_SLA_TARGET_MS),
        );

        assert_eq!(
            serde_json::to_value(error_envelope).expect("serialize error"),
            json!({
                "schema_version": ERROR_SCHEMA_VERSION,
                "tool": SEARCH_SESSIONS_TOOL,
                "request": {
                    "query": "   "
                },
                "error": {
                    "code": "invalid_request",
                    "message": "query must be a non-empty string",
                    "details": {
                        "field": "query"
                    }
                },
                "warnings": [],
                "performance": {
                    "elapsed_ms": 2,
                    "sla_target_ms": 750,
                    "met_sla": true
                }
            })
        );
    }

    #[test]
    fn performance_and_time_helpers_follow_contract() {
        assert_eq!(
            Performance::from_elapsed_ms(500, OPEN_SLA_TARGET_MS),
            Performance {
                elapsed_ms: 500,
                sla_target_ms: 500,
                met_sla: true
            }
        );
        assert!(!Performance::from_elapsed_ms(501, OPEN_SLA_TARGET_MS).met_sla);
        assert_eq!(format_rfc3339_utc_millis(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_rfc3339_utc_millis(1_777_488_751_125),
            "2026-04-29T18:52:31.125Z"
        );
    }
}
