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
    #[error("transport error: {reason}")]
    TransportError { reason: String },

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

    /// Generic configuration validation failure (non-bootstrap path).
    #[error("invalid configuration: {reason}")]
    InvalidConfig { reason: String },

    /// Existing persisted data was found but is inconsistent and cannot be
    /// recovered automatically. Operator intervention is required.
    #[error("recovery required; persisted state is inconsistent")]
    RecoveryRequired,

    /// `bootstrap()` was called on a node that has already been bootstrapped
    /// or recovered.
    #[error("already bootstrapped: {reason}")]
    AlreadyBootstrapped { reason: String },

    /// Election-related operation invoked from an invalid role/state.
    #[error("invalid election state: {reason}")]
    InvalidElectionState { reason: String },
}

/// Convenience alias used throughout xraft.
pub type Result<T> = std::result::Result<T, XraftError>;
