use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::Term;
use crate::voter::VotersRecord;

/// Discriminator for log entry types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command.
    Command,
    /// Leader no-op appended at the start of a new term.
    LeaderChangeMessage,
    /// Membership change — contains a complete `VotersRecord`.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: u64,
    pub term: Term,
    /// Type discriminator.
    pub entry_type: EntryType,
    /// Serialised payload (command bytes or control record).
    pub payload: Vec<u8>,
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
