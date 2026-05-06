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
    /// Create a VotersRecord log entry at the given offset and term.
    pub fn voters_record(offset: u64, term: Term, record: &VotersRecord) -> Self {
        let payload = bincode::serialize(record).expect("VotersRecord serialisation");
        LogEntry {
            offset,
            term,
            entry_type: EntryType::VotersRecord,
            payload,
        }
    }

    /// Create a Command log entry.
    pub fn command(offset: u64, term: Term, data: Vec<u8>) -> Self {
        LogEntry {
            offset,
            term,
            entry_type: EntryType::Command,
            payload: data,
        }
    }
}
