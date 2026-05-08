use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Term};
use crate::voter::VoterInfo;

/// Node role in the Raft protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Initial state before bootstrap or recovery; also terminal state
    /// for a removed voter.
    Unattached,
    /// Passive participant that accepts log entries from the leader.
    Follower,
    /// Actively seeking votes to become leader.
    Candidate,
    /// Active leader replicating log entries to followers.
    Leader,
}

/// Public projection of protocol state returned by `RaftNode::read()`.
///
/// Contains only the subset of `NodeState` fields that are safe to expose.
/// This is a separate type from the internal `NodeState`.
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
