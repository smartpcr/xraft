use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque application command payload. xraft never interprets its contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

/// Opaque application snapshot payload. xraft never interprets its contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
