use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// The type of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application command wrapping an AppRecord.
    Command,
    /// Leader change message (control record).
    LeaderChangeMessage,
    /// Voter set change (control record).
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The offset of this entry in the log.
    pub offset: u64,
    /// The term when this entry was created.
    pub term: u64,
    /// The type of entry.
    pub entry_type: EntryType,
    /// Serialized payload (bincode-encoded AppRecord, VotersRecord, etc.).
    pub payload: Vec<u8>,
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
