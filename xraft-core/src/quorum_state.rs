use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Term};

/// Persisted voting state (quorum-state file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: Term,
}
