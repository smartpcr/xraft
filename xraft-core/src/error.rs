use thiserror::Error;

/// Top-level error type for xraft.
#[derive(Error, Debug)]
pub enum XraftError {
    #[error("storage error: {0}")]
    StorageError(#[from] std::io::Error),

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

    #[error("corruption: {0}")]
    Corruption(String),
}

/// Convenience Result type.
pub type Result<T> = std::result::Result<T, XraftError>;
