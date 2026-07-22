use thiserror::Error;

pub type RepoResult<T> = Result<T, RepoError>;

#[derive(Debug, Clone, Error)]
pub enum RepoError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid cursor: {0}")]
    InvalidCursor(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("publication read model changed during request")]
    ReadModelChanged,
    /// The operation's query budget deadline expired (locally refused by the
    /// query envelope, or killed server-side by `max_execution_time`).
    /// Classified at the repository boundary from typed transport errors —
    /// never from scope/auth/not-found outcomes, which travel as
    /// `Ok(None)`/empty results (amendment A11).
    #[error("deadline exceeded: {budget_note}")]
    DeadlineExceeded { budget_note: String },
    /// The operation exhausted a non-time budget: statement cap, cumulative
    /// read allowance, or a server memory/rows/bytes ceiling.
    #[error("resource exhausted: {budget_note}")]
    ResourceExhausted { budget_note: String },
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

    pub fn deadline_exceeded(budget_note: impl Into<String>) -> Self {
        Self::DeadlineExceeded {
            budget_note: budget_note.into(),
        }
    }

    pub fn resource_exhausted(budget_note: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            budget_note: budget_note.into(),
        }
    }
}
