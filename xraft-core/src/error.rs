use std::io;

/// Top-level error type for xraft.
#[derive(Error, Debug)]
pub enum XraftError {
    StorageError(String),
    TransportError(String),

    #[error("not leader")]
    NotLeader,
    StorageError(String),
    TransportError(String),
    Shutdown,

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("corruption: {0}")]
    Corruption(String),
}

/// Convenience Result type.
pub type Result<T> = std::result::Result<T, XraftError>;
