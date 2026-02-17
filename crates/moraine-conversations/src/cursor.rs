use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::error::{RepoError, RepoResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationCursor {
    pub last_event_unix_ms: i64,
    pub session_id: String,
    pub filter_sig: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCursor {
    pub last_turn_seq: u32,
    pub session_id: String,
    pub filter_sig: String,
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
