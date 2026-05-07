use std::fmt;
use std::io;

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
            XraftError::StorageError(e) => write!(f, "storage error: {e}"),
            XraftError::TransportError(e) => write!(f, "transport error: {e}"),
            XraftError::NotLeader { leader_id } => {
                write!(f, "not leader (leader: {leader_id:?})")
            }
            XraftError::ProposalQueueFull => write!(f, "proposal queue full"),
            XraftError::InvalidClusterId => write!(f, "invalid cluster id"),
            XraftError::Shutdown => write!(f, "node is shutting down"),
        }
    }
}

impl std::error::Error for XraftError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            XraftError::StorageError(e) | XraftError::TransportError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for XraftError {
    fn from(e: io::Error) -> Self {
        XraftError::StorageError(e)
    }
}
