use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: Offset,
    pub term: Term,
    pub entry_type: EntryType,
}

/// Discriminates application records from consensus control records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EntryType {
    /// Client-submitted command forwarded to the StateMachine.
    Command(AppRecord),
    /// Appended by a new leader to establish commit state for its term.
    LeaderChangeMessage,
    /// Records a membership change (voter set update).
    VotersRecord(VotersRecord),
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
