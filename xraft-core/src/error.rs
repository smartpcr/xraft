use thiserror::Error;

use crate::membership::NodeId;

/// Top-level error type for xraft operations.
#[derive(Debug, Error)]
pub enum XraftError {
    #[error("storage error: {0}")]
    StorageError(String),

    #[error("transport error: {0}")]
    TransportError(String),

    #[error("not leader (leader_id: {leader_id:?})")]
    NotLeader { leader_id: Option<NodeId> },

    #[error("proposal queue full")]
    ProposalQueueFull,

    #[error("invalid cluster id")]
    InvalidClusterId,

    #[error("shutdown")]
    Shutdown,

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("{0}")]
    Other(String),
}

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

impl std::error::Error for XraftError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            XraftError::StorageError(e) | XraftError::TransportError(e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageError(msg) => write!(f, "storage error: {msg}"),
            Self::TransportError(msg) => write!(f, "transport error: {msg}"),
            Self::NotLeader => write!(f, "not leader"),
            Self::ProposalQueueFull => write!(f, "proposal queue full"),
            Self::InvalidClusterId => write!(f, "invalid cluster ID"),
            Self::Shutdown => write!(f, "node is shutting down"),
        }
    }
}

impl std::error::Error for XraftError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            XraftError::StorageError(e) | XraftError::TransportError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for XraftError {
    fn from(e: std::io::Error) -> Self {
        Self::StorageError(e)
    }
}
