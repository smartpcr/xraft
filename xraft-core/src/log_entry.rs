use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// The type of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an `AppRecord`).
    Command,
    /// Appended by a new leader as the first entry of its term.
    LeaderChangeMessage,
    /// Membership change control record.
    VotersRecord,
}

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
