use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// Opaque application command payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    /// Term when the entry was created.
    pub term: Term,
    /// Discriminates command vs. control records.
    pub entry_type: EntryType,
    /// Serialised command or control record.
    pub payload: Bytes,
}

/// Entry type discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an AppRecord).
    Command,
    /// No-op appended by new leader as first entry of its term.
    LeaderChangeMessage,
    /// Encodes complete new voter set for membership changes.
    VotersRecord,
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
