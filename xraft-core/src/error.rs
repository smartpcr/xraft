use std::fmt;

use crate::types::NodeId;

/// Public error type for xraft operations.
#[derive(Debug)]
pub enum XraftError {
    StorageError(io::Error),
    TransportError(io::Error),
    NotLeader { leader_id: Option<NodeId> },
    ProposalQueueFull,
    InvalidClusterId,
    Shutdown,
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            XraftError::StorageError(msg) => write!(f, "storage error: {msg}"),
            XraftError::TransportError(msg) => write!(f, "transport error: {msg}"),
            XraftError::NotLeader => write!(f, "not leader"),
            XraftError::ProposalQueueFull => write!(f, "proposal queue full"),
            XraftError::InvalidClusterId => write!(f, "invalid cluster id"),
            XraftError::Shutdown => write!(f, "node shut down"),
        }
    }
}

impl std::error::Error for XraftError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StorageError(e) | Self::TransportError(e) => Some(e),
            _ => None,
        }
    }
}
