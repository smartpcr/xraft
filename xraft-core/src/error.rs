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
    /// RPC cluster_id mismatch.
    InvalidClusterId,
    /// Node is shutting down; no new operations accepted.
    Shutdown,
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageError(e) => write!(f, "storage error: {e}"),
            Self::TransportError(e) => write!(f, "transport error: {e}"),
            Self::NotLeader { leader_id } => {
                write!(f, "not leader (leader: {leader_id:?})")
            }
            Self::ProposalQueueFull => write!(f, "proposal queue full"),
            Self::InvalidClusterId => write!(f, "invalid cluster id"),
            Self::Shutdown => write!(f, "node is shutting down"),
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
