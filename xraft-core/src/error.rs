use std::fmt;

/// Errors produced by the xraft consensus engine.
#[derive(Debug)]
pub enum XraftError {
    StorageError(String),
    TransportError(String),
    NotLeader,
    ProposalQueueFull,
    InvalidClusterId,
    Shutdown,
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageError(msg) => write!(f, "storage error: {msg}"),
            Self::TransportError(msg) => write!(f, "transport error: {msg}"),
            Self::NotLeader => write!(f, "not leader"),
            Self::ProposalQueueFull => write!(f, "proposal queue full"),
            Self::InvalidClusterId => write!(f, "invalid cluster ID"),
            Self::Shutdown => write!(f, "node is shutting down"),
        }
    }
}

impl std::error::Error for XraftError {}
