use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque application command payload. Never interpreted by xraft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

/// Opaque application snapshot payload. Never interpreted by xraft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
