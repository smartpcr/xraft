use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: u64,
    pub term: Term,
    pub entry_type: EntryType,
    pub payload: Bytes,
}

/// Discriminant for log entry kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an `AppRecord`).
    Command,
    /// Appended by a new leader as the first entry of its term.
    LeaderChangeMessage,
    /// Encodes the complete new voter set for membership changes.
    VotersRecord,
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
