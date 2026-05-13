use std::io;

use crate::types::NodeId;

/// Canonical `Result` alias for all xraft operations.
pub type Result<T> = std::result::Result<T, XraftError>;

/// Public error type for all xraft operations.
#[derive(Debug, thiserror::Error)]
pub enum XraftError {
    /// Log, snapshot, or quorum-state I/O failure surfaced as an `io::Error`.
    ///
    /// Used by real filesystem backends (and the in-memory fault injector,
    /// which constructs an `io::Error` with a synthetic kind/message). The
    /// segment-log integration tests pattern-match this variant and inspect
    /// the inner `e.kind()`.
    #[error("storage error: {0}")]
    StorageError(#[from] io::Error),

    /// Network send/recv failure carrying a human-readable reason.
    #[error("transport error: {0}")]
    TransportError(String),

    /// Bincode (or other) (de)serialization failure.
    #[error("serialization error: {0}")]
    SerializationError(String),

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
