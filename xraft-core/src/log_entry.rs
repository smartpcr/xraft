use crate::app_record::AppRecord;
use crate::types::Term;
use crate::voter::VotersRecord;

/// Entry type discriminator.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum EntryType {
    Command,
    LeaderChangeMessage,
    VotersRecord,
}

/// A single log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub offset: u64,
    pub term: Term,
    pub entry_type: EntryType,
    pub payload: Option<AppRecord>,
}

impl LogEntry {
    /// Decode a `VotersRecord` from a `VotersRecord`-typed entry's payload.
    ///
    /// Returns `None` if the entry is not a `VotersRecord` type or if the
    /// payload is missing/malformed.
    pub fn decode_voters_record(&self) -> Option<VotersRecord> {
        match self.entry_type {
            EntryType::VotersRecord => {
                let payload = self.payload.as_ref()?;
                serde_json::from_slice(&payload.data).ok()
            }
            _ => None,
        }
    }

    /// For a `LeaderChangeMessage` entry, the entry's term **is** the
    /// leader epoch (architecture §5.4 — a leader-change record is
    /// committed at the start of a new leader's term).
    pub fn leader_epoch(&self) -> Option<Term> {
        match self.entry_type {
            EntryType::LeaderChangeMessage => Some(self.term),
            _ => None,
        }
    }
}
