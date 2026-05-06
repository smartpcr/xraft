use std::fmt;

use crate::types::NodeId;

/// Public error type for xraft operations.
#[derive(Debug)]
pub enum XraftError {
    StorageError(io::Error),
    TransportError(io::Error),
    NotLeader { leader_id: Option<NodeId> },
    ProposalQueueFull,

    #[error("invalid cluster id")]
    InvalidClusterId,

    #[error("shutdown")]
    Shutdown,
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            XraftError::StorageError(e) => write!(f, "storage error: {e}"),
            XraftError::TransportError(e) => write!(f, "transport error: {e}"),
            XraftError::NotLeader { leader_id: Some(id) } => {
                write!(f, "not leader; current leader is node {id}")
            }
            XraftError::NotLeader { leader_id: None } => {
                write!(f, "not leader; leader unknown")
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
            Self::StorageError(e) | Self::TransportError(e) => Some(e),
            _ => None,
        }
    }
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
