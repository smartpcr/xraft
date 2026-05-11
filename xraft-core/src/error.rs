use std::fmt;
use std::io;

/// Crate-wide error type for xraft operations.
#[derive(Debug)]
pub enum XraftError {
    /// An I/O error from the storage layer.
    Io(io::Error),
    /// Attempted to apply a log entry that has already been applied.
    AlreadyApplied { index: u64 },
    /// A log entry at the expected index was not found.
    LogEntryNotFound { index: u64 },
    /// The node is not the current leader and cannot service the request.
    NotLeader { leader_hint: Option<u64> },
    /// A message was received from an unknown peer.
    UnknownPeer { peer_id: u64 },
    /// An internal invariant was violated.
    Internal(String),
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::AlreadyApplied { index } => {
                write!(f, "log entry at index {index} already applied")
            }
            Self::LogEntryNotFound { index } => {
                write!(f, "log entry not found at index {index}")
            }
            Self::NotLeader { leader_hint } => match leader_hint {
                Some(id) => write!(f, "not leader; current leader may be node {id}"),
                None => write!(f, "not leader; leader unknown"),
            },
            Self::UnknownPeer { peer_id } => {
                write!(f, "message from unknown peer {peer_id}")
            }
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for XraftError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for XraftError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, XraftError>;
