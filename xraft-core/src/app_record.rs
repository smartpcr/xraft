/// Opaque application command payload. xraft never interprets the contents.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AppRecord {
    pub data: bytes::Bytes,
}

/// Opaque application snapshot payload. xraft never interprets the contents.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
