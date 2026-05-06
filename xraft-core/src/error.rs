use std::fmt;

/// Top-level error type for xraft operations.
#[derive(Debug)]
pub enum XraftError {
    StorageError(std::io::Error),
    TransportError(std::io::Error),
    NotLeader { leader_id: Option<crate::types::NodeId> },
    ProposalQueueFull,
    InvalidClusterId,
    Shutdown,
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageError(e) => write!(f, "storage error: {e}"),
            Self::TransportError(e) => write!(f, "transport error: {e}"),
            Self::NotLeader { leader_id } => write!(f, "not leader (leader: {leader_id:?})"),
            Self::ProposalQueueFull => write!(f, "proposal queue full"),
            Self::InvalidClusterId => write!(f, "invalid cluster id"),
            Self::Shutdown => write!(f, "shutting down"),
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

impl From<std::io::Error> for XraftError {
    fn from(e: std::io::Error) -> Self {
        Self::StorageError(e)
    }
}
