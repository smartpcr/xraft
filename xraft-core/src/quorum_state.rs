use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Persisted quorum state: term, vote, leader info.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: u64,
}
