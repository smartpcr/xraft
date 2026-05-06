use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque application command payload.
///
/// Wraps arbitrary bytes that represent an application-level command.
/// The Raft consensus layer treats this as an opaque blob — only the
/// application's `StateMachine` and `Listener` interpret the contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppRecord {
    pub data: bytes::Bytes,
}

impl AppRecord {
    /// Creates a new `AppRecord` from raw bytes.
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self { data: data.into() }
    }
}

impl Serialize for AppRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.data)
    }
}

impl<'de> Deserialize<'de> for AppRecord {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Self {
            data: Bytes::from(bytes),
        })
    }
}

/// Opaque application snapshot payload.
///
/// Contains a serialised snapshot of the application's state machine.
/// The Raft layer manages snapshot metadata separately; this holds only
/// the application-owned portion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}

impl AppSnapshot {
    /// Creates a new `AppSnapshot` from raw bytes.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}
