use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::app_record::AppRecord;
use crate::types::{Offset, Term};
use crate::voter::VotersRecord;

/// The type of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an `AppRecord`).
    Command,
    /// Appended by a new leader as the first entry of its term.
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
    /// Create a command log entry from a raw payload.
    ///
    /// Accepts a typed `Offset` and `Term` plus the already-serialised
    /// command bytes. The `Offset` is unpacked into the underlying `u64`
    /// field so the rest of the codebase that still works with raw `u64`
    /// offsets does not need a ripple change.
    pub fn command(offset: Offset, term: Term, data: Vec<u8>) -> Self {
        Self {
            offset: offset.0,
            term,
            entry_type: EntryType::Command,
            payload: Bytes::from(data),
        }
    }

    /// Convenience: build a command entry directly from an `AppRecord`.
    pub fn from_app_record(offset: Offset, term: Term, record: &AppRecord) -> Self {
        Self::command(offset, term, record.data.to_vec())
    }

    /// Create a leader change message (no-op) log entry.
    pub fn leader_change(offset: u64, term: Term) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::LeaderChangeMessage,
            payload: Bytes::new(),
        }
    }

    /// Create a `VotersRecord` log entry by bincode-serialising the record.
    ///
    /// Matches the `bincode::deserialize::<VotersRecord>(&entry.payload)`
    /// read paths in `raft_node.rs` and `replication.rs`.
    pub fn voters_record(offset: Offset, term: Term, record: &VotersRecord) -> Self {
        let bytes = bincode::serialize(record).expect("VotersRecord serialisation is infallible");
        Self {
            offset: offset.0,
            term,
            entry_type: EntryType::VotersRecord,
            payload: Bytes::from(bytes),
        }
    }
}
