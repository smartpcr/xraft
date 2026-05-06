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
    pub offset: Offset,
    pub term: Term,
    /// Type discriminator.
    pub entry_type: EntryType,
    /// Serialised command or control record payload.
    pub payload: Bytes,
}

impl LogEntry {
    /// Create a command log entry.
    pub fn command(offset: u64, term: Term, record: &AppRecord) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::Command,
            data: record.data.clone(),
        }
    }

    /// Create a leader change message (no-op) log entry.
    pub fn leader_change(offset: u64, term: Term) -> Self {
        Self {
            offset,
            term,
            entry_type: EntryType::LeaderChangeMessage,
            data: Vec::new(),
        }
    }
}
