use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::model::{Checkpoint, RowBatch};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestCommitKind {
    Append,
    ReplayPrepared,
    ReplayPublished,
    ReplayBlocked,
    Heartbeat,
    Progress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableIngestAck {
    pub kind: IngestCommitKind,
    pub operation_id: String,
    pub checkpoint_revision: u64,
    pub publication_revision: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PreparedIngestBatch {
    pub raw_rows: Vec<Value>,
    pub event_rows: Vec<Value>,
    pub link_rows: Vec<Value>,
    pub tool_rows: Vec<Value>,
    pub error_rows: Vec<Value>,
    pub checkpoint: Option<Checkpoint>,
}

impl From<RowBatch> for PreparedIngestBatch {
    fn from(batch: RowBatch) -> Self {
        Self {
            raw_rows: batch.raw_rows,
            event_rows: batch.event_rows,
            link_rows: batch.link_rows,
            tool_rows: batch.tool_rows,
            error_rows: batch.error_rows,
            checkpoint: batch.checkpoint,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PublicationTransition {
    pub source_name: String,
    pub source_file: String,
    pub source_generation: u32,
    pub operation_id: String,
    pub checkpoint_revision: u64,
    pub publication_revision: u64,
    pub cache_epoch: u64,
    pub checkpoint: Checkpoint,
}

#[derive(Debug, Clone)]
pub enum IngestWrite {
    Append(PreparedIngestBatch),
    BeginReplay(PublicationTransition),
    PublishReplay {
        transition: PublicationTransition,
        batch: Box<PreparedIngestBatch>,
    },
    BlockReplay {
        transition: PublicationTransition,
        reason: String,
    },
    Heartbeat(Value),
    Progress(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestStoreErrorKind {
    Unavailable,
    Busy,
    ResourceExhausted,
    Cancelled,
    Shutdown,
    SchemaIncompatible,
    Invariant,
    Corruption,
    Permanent,
}

#[derive(Debug, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct IngestStoreError {
    pub kind: IngestStoreErrorKind,
    pub message: String,
}

impl IngestStoreError {
    pub fn new(kind: IngestStoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self.kind,
            IngestStoreErrorKind::Unavailable
                | IngestStoreErrorKind::Busy
                | IngestStoreErrorKind::ResourceExhausted
        )
    }
}

#[async_trait]
pub trait IngestStore: Send + Sync {
    async fn validate_capabilities(self: Arc<Self>) -> Result<(), IngestStoreError>;
    async fn load_checkpoints(self: Arc<Self>) -> Result<Vec<Checkpoint>, IngestStoreError>;
    async fn write(
        self: Arc<Self>,
        command: IngestWrite,
    ) -> Result<DurableIngestAck, IngestStoreError>;
    async fn shutdown(self: Arc<Self>) -> Result<(), IngestStoreError>;
}

#[cfg(test)]
mod tests {
    use super::{IngestStoreError, IngestStoreErrorKind};

    #[test]
    fn retry_classification_is_fail_closed() {
        for kind in [
            IngestStoreErrorKind::Unavailable,
            IngestStoreErrorKind::Busy,
            IngestStoreErrorKind::ResourceExhausted,
        ] {
            assert!(IngestStoreError::new(kind, "temporary").retryable());
        }
        for kind in [
            IngestStoreErrorKind::Cancelled,
            IngestStoreErrorKind::Shutdown,
            IngestStoreErrorKind::SchemaIncompatible,
            IngestStoreErrorKind::Invariant,
            IngestStoreErrorKind::Corruption,
            IngestStoreErrorKind::Permanent,
        ] {
            assert!(!IngestStoreError::new(kind, "terminal").retryable());
        }
    }
}
