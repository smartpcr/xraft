use std::io;

use crate::membership::NodeId;

/// Unified error type for all xraft public APIs.
#[derive(Debug)]
pub enum XraftError {
    /// Log, snapshot, or quorum-state I/O failure.
    #[error("storage error: {0}")]
    StorageError(#[from] io::Error),

    /// Network send/recv failure.
    TransportError(io::Error),
    /// `propose()` called on a non-leader node.
    NotLeader { leader_id: Option<NodeId> },

    /// BatchAccumulator back-pressure limit reached.
    #[error("proposal queue full")]
    ProposalQueueFull,

    /// RPC cluster_id mismatch.
    #[error("invalid cluster id")]
    InvalidClusterId,

    /// Node is shutting down; no new operations accepted.
    #[error("node is shutting down")]
    Shutdown,

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

    /// Bootstrap input validation failure.
    #[error("invalid bootstrap configuration: {reason}")]
    InvalidBootstrapConfig { reason: String },

/// Convenience alias used throughout xraft.
pub type Result<T> = std::result::Result<T, XraftError>;
