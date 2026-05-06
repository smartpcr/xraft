use crate::types::{NodeId, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: u64,
}

impl QuorumState {
    pub fn new() -> Self {
        Self {
            current_term: Term(0),
            voted_for: None,
            leader_id: None,
            leader_epoch: 0,
        }
    }
}

impl Default for QuorumState {
    fn default() -> Self {
        Self::new()
    }
}
