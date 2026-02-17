use thiserror::Error;

pub type RepoResult<T> = Result<T, RepoError>;

#[derive(Debug, Error)]
pub enum RepoError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid cursor: {0}")]
    InvalidCursor(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl RepoError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    pub fn invalid_cursor(message: impl Into<String>) -> Self {
        Self::InvalidCursor(message.into())
    }

    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}
