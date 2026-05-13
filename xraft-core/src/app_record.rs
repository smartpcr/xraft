use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque application command payload. xraft never interprets its contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: Bytes,
}

impl AppRecord {
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self { data: data.into() }
    }
}

/// Opaque application snapshot payload. xraft never interprets its contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
