use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Offset, Term};
use crate::voter::VoterInfo;

/// The role a node currently occupies in the Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Initial state before bootstrap or recovery completes.
    Unattached,
    /// Following a known leader; replicates log via Fetch.
    Follower,
    /// Running an election (pre-vote or real).
    Candidate,
    /// Accepted as leader for the current term.
    Leader,
}

/// Public projection of protocol state returned by `RaftNode::read()`.
/// Contains only the fields an application needs to observe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub log_end_offset: Offset,
    pub high_watermark: Offset,
    pub voter_set: Vec<VoterInfo>,
}
