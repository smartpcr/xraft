use crate::types::{NodeId, Term};
use crate::voter::VoterInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Unattached,
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
    pub voter_set: Vec<VoterInfo>,
}
