use std::io;

/// Error types for the xraft system.
#[derive(Debug)]
pub enum XraftError {
    /// Storage layer error (log, snapshot, quorum state).
    StorageError(String),
    /// Transport layer error (network send/receive).
    TransportError(String),
    /// Node is not the leader; cannot process this request.
    NotLeader,
    /// Proposal queue is full.
    ProposalQueueFull,
    /// Cluster ID mismatch.
    InvalidClusterId,
    /// Node is shutting down.
    Shutdown,

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageError(msg) => write!(f, "storage error: {msg}"),
            Self::TransportError(msg) => write!(f, "transport error: {msg}"),
            Self::NotLeader => write!(f, "not leader"),
            Self::ProposalQueueFull => write!(f, "proposal queue full"),
            Self::InvalidClusterId => write!(f, "invalid cluster id"),
            Self::Shutdown => write!(f, "node is shutting down"),
        }
    }
}

    /// Bootstrap input validation failure.
    #[error("invalid bootstrap configuration: {reason}")]
    InvalidBootstrapConfig { reason: String },

/// Alias for results using XraftError.
pub type Result<T> = std::result::Result<T, XraftError>;
