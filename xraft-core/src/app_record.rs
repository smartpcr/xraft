use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque application command payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

/// Opaque application snapshot payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
