use crate::types::{NodeId, Term, Role, VoterInfo};

/// Public consensus state returned by `RaftNode::read()`.
#[derive(Debug, Clone)]
pub struct ConsensusState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
    pub voter_set: Vec<VoterInfo>,
}
