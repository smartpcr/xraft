use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::app_record::AppRecord;
use crate::types::{Offset, Term};

/// Distinguishes ordinary command entries from leader-change marker entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogEntryKind {
    Command,
    LeaderChange,
}

/// A single entry of the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: Offset,
    pub term: Term,
    pub kind: LogEntryKind,
    pub payload: Bytes,
}

impl LogEntry {
    /// Build a `Command` entry from an `AppRecord`.
    pub fn command(offset: Offset, term: Term, record: &AppRecord) -> Self {
        Self {
            offset,
            term,
            kind: LogEntryKind::Command,
            payload: record.data.clone(),
        }
    }

    /// Build a `LeaderChange` marker entry. Carries no payload.
    pub fn leader_change(offset: Offset, term: Term) -> Self {
        Self {
            offset,
            term,
            kind: LogEntryKind::LeaderChange,
            payload: Bytes::new(),
        }
    }
}
