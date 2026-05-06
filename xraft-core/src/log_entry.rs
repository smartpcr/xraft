use crate::app_record::AppRecord;
use crate::types::Term;
use crate::voter::VotersRecord;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Discriminator for log entry types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level command wrapping an AppRecord.
    Command,
    /// No-op appended by new leader to establish commit state.
    LeaderChangeMessage,
    /// Membership change record encoding complete new voter set.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    /// Term when the entry was created.
    pub term: Term,
    /// Type discriminator.
    pub entry_type: EntryType,
    /// Serialised payload.
    pub payload: Bytes,
}

impl LogEntry {
    /// Create a Command entry wrapping an AppRecord.
    pub fn command(offset: u64, term: Term, record: &AppRecord) -> Self {
        LogEntry {
            offset,
            term,
            entry_type: EntryType::Command,
            payload: record.data.clone(),
        }
    }

    /// Create a LeaderChangeMessage (no-op) entry.
    pub fn leader_change(offset: u64, term: Term) -> Self {
        LogEntry {
            offset,
            term,
            entry_type: EntryType::LeaderChangeMessage,
            payload: Bytes::new(),
        }
    }

    /// Create a VotersRecord entry.
    pub fn voters_record(offset: u64, term: Term, record: &VotersRecord) -> Self {
        let data = bincode::serialize(record).expect("VotersRecord serialisation");
        LogEntry {
            offset,
            term,
            entry_type: EntryType::VotersRecord,
            payload: Bytes::from(data),
        }
    }

    /// Extract AppRecord from a Command entry. Panics if not Command type.
    pub fn as_app_record(&self) -> AppRecord {
        AppRecord {
            data: self.payload.clone(),
        }
    }
}
