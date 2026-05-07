use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// The type of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an `AppRecord`).
    Command,
    /// Appended by a new leader as the first entry of its term.
    LeaderChangeMessage,
    /// Encodes the complete new voter set for membership changes.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    /// Term when the entry was created.
    pub term: Term,
    /// Discriminator for entry content.
    pub entry_type: EntryType,
    /// Serialised command or control record.
    pub payload: Bytes,
}

impl LogEntry {
    /// Create a command log entry.
    pub fn command(offset: u64, term: Term, record: &AppRecord) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::Command,
            data: record.data.clone(),
        }
    }

    /// Create a leader change message (no-op) log entry.
    pub fn leader_change(offset: u64, term: Term) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::LeaderChangeMessage,
            data: Vec::new(),
        }
    }
}
