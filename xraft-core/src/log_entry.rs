use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// The type of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an `AppRecord`).
    Command,
    /// Leader no-op appended at the start of a new term.
    LeaderChangeMessage,
    /// Encodes the complete new voter set for membership changes.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    /// Term when the entry was created.
    pub term: Term,
    /// Discriminator for entry content.
    pub entry_type: EntryType,
    /// Serialised command or control record.
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
