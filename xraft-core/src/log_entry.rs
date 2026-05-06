use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// The type of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    Command,
    LeaderChangeMessage,
    /// Membership change control record.
    VotersRecord,
}

/// A single log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub offset: u64,
    pub term: Term,
    pub entry_type: EntryType,
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
