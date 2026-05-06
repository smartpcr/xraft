use bytes::Bytes;
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: u64,
    pub term: u64,
    pub entry_type: EntryType,
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
