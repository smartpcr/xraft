use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Term, VoterInfo};

/// Role of a node in the consensus protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Initial state before bootstrap or recovery completes.
    Unattached,
    /// Following a known leader.
    Follower,
    /// Running an election.
    Candidate,
    /// Serving as cluster leader.
    Leader,
}

/// Public projection of NodeState, returned by `RaftNode::read()`.
/// Contains only the fields safe to expose externally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub log_end_offset: u64,
    pub high_watermark: u64,
    pub voter_set: Vec<VoterInfo>,
}
