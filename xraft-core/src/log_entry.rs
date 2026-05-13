use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::Term;

/// Discriminator for log entry types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// Application-level state machine command (wraps an AppRecord).
    Command,
    /// Appended by a new leader as the first entry of its term.
    LeaderChangeMessage,
    /// Encodes the complete new voter set.
    VotersRecord,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (0-indexed).
    pub offset: u64,
    /// Term when the entry was created.
    pub term: Term,
    /// Type discriminator.
    pub entry_type: EntryType,
    /// Serialised command or control record.
    pub payload: Bytes,
}
