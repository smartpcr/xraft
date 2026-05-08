use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::consensus_state::{ConsensusState, Role};
use crate::types::{ClusterId, NodeId, Term};
use crate::voter::VoterInfo;

/// Tracks a pending (uncommitted) membership change.
#[derive(Debug, Clone)]
pub struct PendingMembershipChange {
    pub offset: u64,
    pub voters: Vec<VoterInfo>,
}

/// Full internal protocol state. `pub(crate)` visibility.
#[derive(Debug)]
pub(crate) struct NodeState {
    pub node_id: NodeId,
    pub cluster_id: ClusterId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub role: Role,
    pub leader_id: Option<NodeId>,

    // Log boundaries
    pub log_start_offset: u64,
    pub log_end_offset: u64,
    /// Exclusive upper bound of committed offsets.
    pub high_watermark: u64,

    // Voter set (from latest committed VotersRecord or snapshot)
    pub voter_set: Vec<VoterInfo>,
    pub observers: HashSet<NodeId>,

    // Pending membership change (leader-only)
    pub pending_membership_change: Option<PendingMembershipChange>,

    // Leader-only: per-follower replication progress
    pub follower_state: HashMap<NodeId, crate::follower_progress::FollowerProgress>,

    // Election state
    pub election_deadline: Instant,
    pub votes_received: HashSet<NodeId>,
    pub pre_votes_received: HashSet<NodeId>,
    pub check_quorum_deadline: Instant,
}

impl NodeState {
    /// Create initial Unattached state for a fresh node.
    pub fn new_unattached(node_id: NodeId) -> Self {
        let now = Instant::now();
        Self {
            node_id,
            cluster_id: ClusterId(uuid::Uuid::nil()),
            current_term: Term::ZERO,
            voted_for: None,
            role: Role::Unattached,
            leader_id: None,
            log_start_offset: 0,
            log_end_offset: 0,
            high_watermark: 0,
            voter_set: Vec::new(),
            observers: HashSet::new(),
            pending_membership_change: None,
            follower_state: HashMap::new(),
            election_deadline: now,
            votes_received: HashSet::new(),
            pre_votes_received: HashSet::new(),
            check_quorum_deadline: now,
        }
    }

    /// Project internal state into the public `ConsensusState`.
    pub fn project(&self) -> ConsensusState {
        ConsensusState {
            node_id: self.node_id,
            current_term: self.current_term,
            role: self.role,
            leader_id: self.leader_id,
            log_end_offset: self.log_end_offset,
            high_watermark: self.high_watermark,
            voter_set: self.voter_set.clone(),
        }
    }
}
