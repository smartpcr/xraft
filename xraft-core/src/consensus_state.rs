use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Term};
use crate::voter::VoterInfo;

/// Role in the Raft protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Initial state before bootstrap or recovery completes.
    Unattached,
    Follower,
    Candidate,
    Leader,
}

/// Public projected subset of internal `NodeState`.
///
/// Returned by `RaftNode::read()`. Contains only protocol metadata
/// fields — internal-only fields are not exposed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusState {
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    /// Exclusive upper bound: entry at offset O is committed when O < HW.
    pub high_watermark: u64,
    pub log_end_offset: u64,
    /// Committed voter set.
    pub voter_set: Vec<VoterInfo>,
    pub node_id: NodeId,
}
