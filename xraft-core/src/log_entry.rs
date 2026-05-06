use crate::app_record::AppRecord;
use crate::types::{Offset, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub payload: Vec<u8>,
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
