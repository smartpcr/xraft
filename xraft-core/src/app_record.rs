use serde::{Deserialize, Serialize};

/// Opaque application snapshot payload. xraft never interprets its contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSnapshot {
    pub data: Vec<u8>,
}
