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
/// `version` identifies the READ PATH that minted the token, because the two
/// paths anchor on different values. The projected-header path anchors on
/// `mcp_open_publication_headers.last_event_time`, the projector's exact
/// aggregate; the #599 directory path anchors on
/// `max(mcp_session_directory.max_observed_event_time)` over the session's live
/// source generations, which is also the `updated_at` that path reports. The
/// two coincide for a session that was never re-ingested and diverge otherwise
/// — a `SimpleAggregateFunction(max)` cannot retract a display time that a
/// retry-replay lowered, a `ReplacingMergeTree` read `FINAL` can.
///
/// A token is therefore only meaningful to the path that minted it: resumed on
/// the other path it would silently skip every session whose two values
/// straddle the anchor. A cross-path token is rejected with the cursor-mismatch
/// error rather than resumed, so a readiness flip mid-pagination surfaces as an
/// error the caller can restart from instead of a gap it cannot see.
///
/// An absent `version` key decodes to
/// [`MCP_SESSION_LIST_CURSOR_VERSION_HEADERS`], which is what a pre-#599 token
/// is — that build served every page from the header path.
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

/// Token version minted by the projected-header path — `0`, the value an absent
/// `version` key decodes to, so every pre-#599 token is a header token.
pub const MCP_SESSION_LIST_CURSOR_VERSION_HEADERS: u8 = 0;

/// Token version minted by the #599 directory path.
pub const MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY: u8 = 2;

/// Token versions this build can decode. A version outside this set is an
/// invalid cursor; a version inside it that names the other path is a cursor
/// mismatch (see [`McpSessionListCursor`]).
pub const ACCEPTED_MCP_SESSION_LIST_CURSOR_VERSIONS: [u8; 2] = [
    MCP_SESSION_LIST_CURSOR_VERSION_HEADERS,
    MCP_SESSION_LIST_CURSOR_VERSION_DIRECTORY,
];

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
