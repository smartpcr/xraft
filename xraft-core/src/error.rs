use std::fmt;
use std::io;

/// Top-level error type for all xraft operations.
#[derive(Debug)]
pub enum XraftError {
    /// Log, snapshot, or quorum-state I/O failure.
    StorageError(io::Error),
    /// Network send/recv failure.
    TransportError(io::Error),
    /// propose() called on a non-leader node.
    NotLeader { leader_id: Option<u64> },
    /// BatchAccumulator back-pressure limit reached.
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

impl From<io::Error> for XraftError {
    fn from(e: io::Error) -> Self {
        Self::StorageError(e)
    }
}
