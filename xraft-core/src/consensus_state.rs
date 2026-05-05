use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Term};
use crate::voter::VoterInfo;

/// The role a node occupies in the Raft protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Role {
    /// Initial state before bootstrap or recovery completes.
    Unattached,
    Follower,
    Candidate,
    Leader,
}

/// Public projected view of protocol state, returned by `RaftNode::read()`.
///
/// This is a **separate, smaller struct** from the internal `NodeState`. It
/// contains only the fields safe and useful for external callers making
/// routing or leadership decisions. Internal-only fields (`voted_for`,
/// `cluster_id`, `log_start_offset`, `observers`, `pending_membership_change`,
/// `follower_state`, election/quorum deadlines, vote counters) are not exposed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    /// Exclusive upper bound of committed offsets; entries with
    /// offset < high_watermark are committed.
    pub high_watermark: u64,
    pub log_end_offset: u64,
    /// The committed voter set — does NOT include pending membership changes.
    pub voter_set: Vec<VoterInfo>,
}
