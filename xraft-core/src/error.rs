use std::fmt;

/// Unified error type for xraft operations.
#[derive(Debug)]
pub enum XraftError {
    StorageError(String),
    TransportError(String),
    NotLeader,
    ProposalQueueFull,
    InvalidClusterId,
    Shutdown,
    SerializationError(String),
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageError(msg) => write!(f, "storage error: {msg}"),
            Self::TransportError(msg) => write!(f, "transport error: {msg}"),
            Self::NotLeader => write!(f, "not the leader"),
            Self::ProposalQueueFull => write!(f, "proposal queue full"),
            Self::InvalidClusterId => write!(f, "invalid cluster id"),
            Self::Shutdown => write!(f, "node is shutting down"),
            Self::SerializationError(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for XraftError {}

impl From<std::io::Error> for XraftError {
    fn from(e: std::io::Error) -> Self {
        Self::TransportError(e.to_string())
    }
}

impl From<bincode::Error> for XraftError {
    fn from(e: bincode::Error) -> Self {
        Self::SerializationError(e.to_string())
    }
}
