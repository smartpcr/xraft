use std::fmt;

/// Errors that can occur in the xraft system.
#[derive(Debug)]
pub enum XraftError {
    NotLeader,
    StorageError(String),
    TransportError(String),
    Shutdown,
    ProposalQueueFull,
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            XraftError::NotLeader => write!(f, "not leader"),
            XraftError::StorageError(s) => write!(f, "storage error: {s}"),
            XraftError::TransportError(s) => write!(f, "transport error: {s}"),
            XraftError::Shutdown => write!(f, "node shut down"),
            XraftError::ProposalQueueFull => write!(f, "proposal queue full"),
        }
    }
}

impl std::error::Error for XraftError {}
