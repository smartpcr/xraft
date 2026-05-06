use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// Type of log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application command (forwarded to StateMachine).
    Command,
    /// Control record appended on leader election.
    LeaderChangeMessage,
    /// Control record for voter-set changes.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: Offset,
    pub term: Term,
    pub entry_type: EntryType,
    pub payload: Vec<u8>,
}
