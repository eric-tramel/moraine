#![recursion_limit = "512"]
#![allow(clippy::collapsible_if, clippy::too_many_arguments)]

use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use moraine_conversations::{
    AnalyticsRange, ConversationListFilter, ConversationListSort, ConversationMode,
    ConversationRepository, ConversationSearchQuery, FileAttentionQuery, McpEventType,
    McpSessionListFilter, PageRequest, QueryCause, QueryOwner, QueryWorkload, RepoError,
    SearchEventKind, SearchEventsQuery, SearchMcpEventsQuery, SessionAnalyticsQuery,
    SessionEventsDirection, SessionEventsQuery, SessionLookback, SessionMetadataSearchQuery,
    SessionStep, StoreProbe, TablePreviewQuery, TurnListFilter,
};
use serde_json::json;
use tokio::sync::Notify;
use uuid::Uuid;

mod analytics;
mod cache;
mod file_attention;
mod health;
mod heartbeat;
mod ownership;
mod search;
mod sessions;
mod support;
mod tables;

use support::*;
