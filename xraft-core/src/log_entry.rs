use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::Term;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntryType {
    NoOp,
    Application,
    Configuration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub offset: u64,
    pub term: Term,
    pub entry_type: EntryType,
    #[serde(with = "crate::bytes_serde")]
    pub payload: Bytes,
}

impl LogEntry {
    pub fn new(offset: u64, term: Term, entry_type: EntryType, payload: Bytes) -> Self {
        Self {
            offset,
            term,
            entry_type,
            payload,
        }
    }

    pub fn no_op(offset: u64, term: Term) -> Self {
        Self::new(offset, term, EntryType::NoOp, Bytes::new())
    }
}
