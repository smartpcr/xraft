use std::fmt;
use std::io;

use crate::types::NodeId;

/// Unified error type for all xraft public APIs.
#[derive(Debug)]
pub enum XraftError {
    /// Log, snapshot, or quorum-state I/O failure.
    StorageError(io::Error),
    /// Network send/recv failure.
    TransportError(io::Error),
    /// `propose()` called on a non-leader node.
    NotLeader { leader_id: Option<NodeId> },
    /// BatchAccumulator back-pressure limit reached.
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
            XraftError::StorageError(e) | XraftError::TransportError(e) => Some(e),
            _ => None,
        }
    }
}

/// Convenience alias used throughout xraft.
pub type Result<T> = std::result::Result<T, XraftError>;
