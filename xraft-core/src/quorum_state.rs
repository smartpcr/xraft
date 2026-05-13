use serde::{Deserialize, Serialize};

/// Persisted voting state for crash recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub leader_id: Option<u64>,
    pub leader_epoch: u64,
}
