use thiserror::Error;

pub type RepoResult<T> = Result<T, RepoError>;

#[derive(Debug, Clone, Error)]
pub enum RepoError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid cursor: {0}")]
    InvalidCursor(String),
    #[error("query cancelled: {0}")]
    Cancelled(String),
    #[error("query deadline exceeded: {0}")]
    DeadlineExceeded(String),
    #[error("query resources exhausted: {0}")]
    ResourceExhausted(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("search read model changed during ingest")]
    ReadModelChanged,
}

impl RepoError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    pub fn invalid_cursor(message: impl Into<String>) -> Self {
        Self::InvalidCursor(message.into())
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::Cancelled(message.into())
    }

    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::DeadlineExceeded(message.into())
    }

    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::ResourceExhausted(message.into())
    }

    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}
