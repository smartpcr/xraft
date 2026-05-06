use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque application command payload. xraft never interprets the contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

/// Opaque serialised state machine state for snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
