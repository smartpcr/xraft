use serde::{Deserialize, Serialize};

use crate::types::Term;

/// Identifies a snapshot by its last included offset and epoch (term).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: Term,
}
