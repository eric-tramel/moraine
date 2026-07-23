#![recursion_limit = "512"]
#![allow(clippy::collapsible_if, clippy::too_many_arguments)]

use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use moraine_conversations::{
    AnalyticsRange, ConversationListFilter, ConversationListSort, ConversationMode,
    ConversationRepository, ConversationSearchQuery, FileAttentionQuery, McpEventType,
    McpSessionListFilter, PageRequest, QueryClass, QueryEnvelope, RepoError, SearchEventKind,
    SearchEventsQuery, SearchMcpEventsQuery, SessionAnalyticsQuery, SessionEventsDirection,
    SessionEventsQuery, SessionLookback, SessionMetadataSearchQuery, SessionStep, StoreProbe,
    TablePreviewQuery, TurnListFilter,
};
use serde_json::json;
use tokio::sync::Notify;

mod analytics;
mod cache;
mod errors;
mod file_attention;
mod health;
mod heartbeat;
mod search;
mod sessions;
mod support;
mod tables;

use support::*;
