use std::io;

use thiserror::Error;

use crate::types::NodeId;

/// Unified error type for all xraft public APIs.
#[derive(Debug, Error)]
pub enum XraftError {
    /// Log, snapshot, or quorum-state I/O failure.
    #[error("storage error: {0}")]
    StorageError(#[from] io::Error),

    /// Network send/recv failure.
    #[error("transport error: {0}")]
    TransportError(io::Error),

    /// `propose()` called on a non-leader node.
    #[error("not leader; current leader is {leader_id:?}")]
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

    /// Bootstrap input validation failure.
    #[error("invalid bootstrap configuration: {reason}")]
    InvalidBootstrapConfig { reason: String },
}

/// Convenience alias used throughout xraft.
pub type Result<T> = std::result::Result<T, XraftError>;
