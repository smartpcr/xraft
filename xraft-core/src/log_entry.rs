use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Offset, Term};

/// Discriminant for log entry types.
///
/// The log contains two classes of entries:
/// - `Command` — application-level state machine command (wraps an `AppRecord`).
///   The only entry type delivered to `StateMachine::apply`.
/// - `LeaderChangeMessage` — appended by a new leader as the first entry of its
///   term to establish commit state.
/// - `VotersRecord` — appended when processing membership changes. Encodes the
///   complete new voter set.
///
/// Control records (`LeaderChangeMessage`, `VotersRecord`) are never exposed to
/// the application's `StateMachine`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntryType {
    Command,
    LeaderChangeMessage,
    VotersRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: Offset,
    pub term: Term,
    pub entry_type: EntryType,
    /// Serialised command or control record payload.
    pub payload: Bytes,
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
