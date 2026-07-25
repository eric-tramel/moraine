use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::domain::{ConversationListSort, SessionEventsDirection};
use crate::error::{RepoError, RepoResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationCursor {
    pub last_event_unix_ms: i64,
    pub session_id: String,
    pub filter_sig: String,
    #[serde(default)]
    pub sort: ConversationListSort,
}

/// Keyset anchor over `(updated_at, session_id)` for `list_sessions`.
///
/// `version` separates the pre-#599 projected-header token (key absent, decoded
/// as `0`) from the #599 directory token (`2`). Any other value is rejected as
/// an invalid cursor.
///
/// Both are accepted because the anchor is the same `cd::DISPLAY_TIME_EXPR`
/// maximum under either implementation — the header path projects it, the
/// directory path re-aggregates it. The two differ only where an event version
/// has been superseded: the directory's `SimpleAggregateFunction(max)` cannot
/// retract one, so a v2 anchor can sit above the header path's exact value.
/// Resuming a v0 token on the directory path may therefore re-serve the
/// boundary session, the same duplicate class the moving-feed contract already
/// accepts across a #602 generation replay. WITHIN one implementation the
/// anchor is always the value that implementation filters on, which is what
/// makes paging skip-free.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpSessionListCursor {
    #[serde(default)]
    pub version: u8,
    pub last_event_unix_ms: i64,
    pub session_id: String,
    pub filter_sig: String,
    #[serde(default)]
    pub sort: ConversationListSort,
}

/// The token version this build mints.
pub const MCP_SESSION_LIST_CURSOR_VERSION: u8 = 2;

/// Token versions this build accepts: the legacy header token (`0`) and the
/// directory token ([`MCP_SESSION_LIST_CURSOR_VERSION`]).
pub const ACCEPTED_MCP_SESSION_LIST_CURSOR_VERSIONS: [u8; 2] = [0, MCP_SESSION_LIST_CURSOR_VERSION];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCursor {
    pub last_turn_seq: u32,
    pub session_id: String,
    pub filter_sig: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEventCursor {
    pub last_event_order: u64,
    pub last_event_uid: String,
    pub session_id: String,
    pub direction: SessionEventsDirection,
    pub filter_sig: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_token: Option<String>,
}

pub fn encode_cursor<T: Serialize>(cursor: &T) -> RepoResult<String> {
    let json = serde_json::to_vec(cursor)
        .map_err(|err| RepoError::internal(format!("failed to serialize cursor: {err}")))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

pub fn decode_cursor<T: for<'de> Deserialize<'de>>(token: &str) -> RepoResult<T> {
    let raw = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|err| RepoError::invalid_cursor(format!("invalid base64 cursor: {err}")))?;
    serde_json::from_slice(&raw)
        .map_err(|err| RepoError::invalid_cursor(format!("invalid cursor payload: {err}")))
}
