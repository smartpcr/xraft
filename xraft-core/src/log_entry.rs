use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    pub term: u64,
    pub entry_type: EntryType,
    /// Serialised command or control record.
    pub payload: Bytes,
}

impl LogEntry {
    /// Create a command log entry.
    pub fn command(offset: Offset, term: Term, data: Vec<u8>) -> Self {
        Self {
            offset: offset.0,
            term: term.0,
            entry_type: EntryType::Command,
            payload: Bytes::from(data),
        }
    }

    /// Create a leader change message (no-op) log entry.
    pub fn leader_change(offset: u64, term: Term) -> Self {
        Self {
            offset,
            term: term.0,
            entry_type: EntryType::LeaderChangeMessage,
            payload: Bytes::new(),
        }
    }
}
