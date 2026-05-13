use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Application-level record carried inside a `LogEntry` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

impl AppRecord {
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self { data: data.into() }
    }
}

/// Snapshot of the application state machine. `PartialEq` + `Eq` are
/// required because `Snapshot` derives them and contains an `AppSnapshot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Bytes,
}

impl AppSnapshot {
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self { data: data.into() }
    }
}
