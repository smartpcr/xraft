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
    /// Application-level state machine command.
    Command,
    /// Leader no-op appended at the start of a new term.
    LeaderChangeMessage,
    /// Membership change — contains a complete `VotersRecord`.
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
    /// Serialised command or control record payload.
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
