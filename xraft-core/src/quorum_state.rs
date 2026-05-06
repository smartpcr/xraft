use crate::types::{NodeId, Term};

/// Persisted voting state, stored separately from the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: Term,
}
