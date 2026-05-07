use std::fmt;
use std::io;

/// Errors produced by xraft operations.
#[derive(Debug)]
pub enum XraftError {
    StorageError(String),
    TransportError(String),
    NotLeader,
    ProposalQueueFull,
    InvalidClusterId,
    Shutdown,

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

pub type Result<T> = std::result::Result<T, XraftError>;
