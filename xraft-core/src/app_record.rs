use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque application command payload. xraft never interprets the contents;
/// it only stores, replicates, and delivers them to the application's
/// `StateMachine`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRecord {
    pub data: bytes::Bytes,
}

/// Opaque application snapshot payload. Produced by `StateMachine::snapshot()`
/// and consumed by `StateMachine::restore()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
