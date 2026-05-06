use thiserror::Error;

/// Top-level error type for xraft operations.
#[derive(Debug, Error)]
pub enum XraftError {
    #[error("storage error: {0}")]
    StorageError(String),

    #[error("transport error: {0}")]
    TransportError(String),

    #[error("not leader")]
    NotLeader,

    #[error("proposal queue full")]
    ProposalQueueFull,

    #[error("invalid cluster id")]
    InvalidClusterId,

    #[error("shutdown")]
    Shutdown,

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, XraftError>;
