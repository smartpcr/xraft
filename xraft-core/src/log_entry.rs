use serde::{Deserialize, Serialize};

/// Type of log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application command wrapping an AppRecord.
    Command,
    /// Leader change message (control record, never exposed to StateMachine).
    LeaderChangeMessage,
    /// Voters record (control record, never exposed to StateMachine).
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    pub term: u64,
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
