use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Term};

/// Persisted voting state. Written to the `quorum-state` file separately
/// from the log to guarantee vote durability across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: Term,
}

impl Default for QuorumState {
    fn default() -> Self {
        Self {
            current_term: Term(0),
            voted_for: None,
            leader_id: None,
            leader_epoch: Term(0),
        }
    }
}
