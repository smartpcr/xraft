use std::fmt;

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
    /// RPC `cluster_id` mismatch.
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

impl std::error::Error for XraftError {}

/// Convenience alias used throughout xraft.
pub type Result<T> = std::result::Result<T, XraftError>;
