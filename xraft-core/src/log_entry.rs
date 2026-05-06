use serde::{Deserialize, Serialize};
use crate::types::{Term, AppRecord};

/// Types of log entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application command (wraps an AppRecord).
    Command,
    /// No-op entry appended by new leader at start of term.
    LeaderChangeMessage,
    /// Membership change record.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    /// Term when the entry was created.
    pub term: Term,
    /// Entry type discriminator.
    pub entry_type: EntryType,
    /// Entry payload — for Command entries this is a serialized AppRecord.
    pub data: Vec<u8>,
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
