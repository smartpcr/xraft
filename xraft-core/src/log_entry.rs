use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// The type of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an `AppRecord`).
    Command,
    /// Leader change marker record.
    LeaderChangeMessage,
    /// Encodes the complete new voter set for membership changes.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-based).
    pub offset: Offset,
    /// Term in which the entry was created.
    pub term: Term,
    /// Discriminator for entry content.
    pub entry_type: EntryType,
    /// Serialised command or control record.
    pub payload: Bytes,
}

impl LogEntry {
    /// Create a command log entry.
    pub fn command(offset: Offset, term: Term, payload: Vec<u8>) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::Command,
            payload,
        }
    }

    /// Create a leader change message entry.
    pub fn leader_change(offset: Offset, term: Term) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::LeaderChangeMessage,
            payload: Vec::new(),
        }
    }
}
