use std::io;

use crate::types::NodeId;

/// Public error type for all xraft operations.
#[derive(Debug, thiserror::Error)]
pub enum XraftError {
    /// Log, snapshot, or quorum-state I/O failure.
    #[error("storage error: {0}")]
    StorageError(#[from] io::Error),

    /// Network send/recv failure.
    #[error("transport error: {reason}")]
    TransportError { reason: String },

    /// propose() called on a non-leader node.
    #[error("not leader, current leader: {leader_id:?}")]
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

    /// Bootstrap rejected: node already initialised.
    #[error("already bootstrapped: {reason}")]
    AlreadyBootstrapped { reason: String },

    /// Bootstrap input validation failure.
    #[error("invalid bootstrap configuration: {reason}")]
    InvalidBootstrapConfig { reason: String },

    /// Configuration validation failure.
    #[error("invalid configuration: {reason}")]
    InvalidConfig { reason: String },

    /// Recovery not yet implemented (placeholder for Stage 6.1).
    #[error("crash recovery required but not yet implemented")]
    RecoveryRequired,

    /// Election attempted from an invalid role.
    #[error("invalid election state: {reason}")]
    InvalidElectionState { reason: String },
}
