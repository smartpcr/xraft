use serde::{Deserialize, Serialize};

use crate::app_record::AppRecord;
use crate::types::{Offset, Term};
use crate::voter::VotersRecord;

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: Offset,
    pub term: Term,
    pub entry_type: EntryType,
}

/// Discriminates application records from consensus control records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EntryType {
    /// Client-submitted command forwarded to the StateMachine.
    Command(AppRecord),
    /// Appended by a new leader to establish commit state for its term.
    LeaderChangeMessage,
    /// Records a membership change (voter set update).
    VotersRecord(VotersRecord),
}
