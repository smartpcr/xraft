use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// Opaque application command payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: Offset,
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
    pub fn command(offset: Offset, term: Term, record: &AppRecord) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::Command,
            payload: record.data.to_vec(),
        }
    }
}
