use std::collections::HashSet;

use crate::consensus_state::{ConsensusState, Role};
use crate::types::{ClusterId, NodeId, Offset, Term};
use crate::voter::VoterInfo;

/// Tracks whether the node is in a pre-vote or real election phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionPhase {
    /// Not running any election.
    None,
    /// Pre-vote in progress — no durable state has been mutated.
    PreVote,
    /// Real election in progress — term incremented and voted_for persisted.
    Election,
}

/// Full internal protocol state (`pub(crate)` in a production build).
/// Contains everything the event loop needs to process messages.
pub struct NodeState {
    pub node_id: NodeId,
    pub cluster_id: ClusterId,

    // Term and vote — the only fields that require fsync-before-ack
    pub current_term: Term,
    pub voted_for: Option<NodeId>,

    pub role: Role,
    pub leader_id: Option<NodeId>,

    // Log bounds
    pub log_start_offset: Offset,
    pub log_end_offset: Offset,
    pub high_watermark: Offset,

    // Term of the last log entry (for vote comparison)
    pub last_log_term: Term,

    // Voter set
    pub voter_set: Vec<VoterInfo>,

    // Election tracking
    pub election_phase: ElectionPhase,
    pub votes_received: HashSet<NodeId>,
    pub pre_votes_received: HashSet<NodeId>,

    /// Timestamp (milliseconds) of last contact from a valid leader.
    /// Used by the pre-vote rejection rule: followers that have recently
    /// heard from a leader reject pre-vote requests to prevent disruptive
    /// elections from partitioned nodes.
    pub last_leader_contact_ms: Option<u64>,

    /// Term of the leader that last contacted us. Combined with `leader_id`
    /// and `last_leader_contact_ms` to validate that the contact was from a
    /// legitimate current-term leader.
    pub last_leader_term: Option<Term>,

    /// The prospective term for the current pre-vote round. Responses are
    /// only counted if they belong to this round, preventing stale pre-vote
    /// grants from a previous round from being tallied.
    pub pre_vote_term: Option<Term>,
}

impl NodeState {
    /// Creates a new `NodeState` with the given identity and voter set.
    /// Starts in the `Follower` role at term 0 with no leader.
    pub fn new(node_id: NodeId, cluster_id: ClusterId, voter_set: Vec<VoterInfo>) -> Self {
        Self {
            node_id,
            cluster_id,
            current_term: Term(0),
            voted_for: None,
            role: Role::Follower,
            leader_id: None,
            log_start_offset: Offset(0),
            log_end_offset: Offset(0),
            high_watermark: Offset(0),
            last_log_term: Term(0),
            voter_set,
            election_phase: ElectionPhase::None,
            votes_received: HashSet::new(),
            pre_votes_received: HashSet::new(),
            last_leader_contact_ms: None,
            last_leader_term: None,
            pre_vote_term: None,
        }
    }

    /// Projects the internal state into the public `ConsensusState`.
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

    /// Returns the `NodeId`s of all voters except this node.
    pub fn other_voters(&self) -> Vec<NodeId> {
        self.voter_set
            .iter()
            .map(|v| v.node_id)
            .filter(|id| *id != self.node_id)
            .collect()
    }

    /// Returns true if the given node is in the current voter set.
    pub fn is_voter(&self, node_id: NodeId) -> bool {
        self.voter_set.iter().any(|v| v.node_id == node_id)
    }

    /// Total number of voters in the cluster (including self).
    pub fn voter_count(&self) -> usize {
        self.voter_set.len()
    }

    /// Majority quorum size: `⌊n/2⌋ + 1`.
    pub fn majority(&self) -> usize {
        self.voter_count() / 2 + 1
    }

    /// Records contact from a valid leader at the given timestamp.
    pub fn record_leader_contact(&mut self, now_ms: u64, leader_id: NodeId, term: Term) {
        self.last_leader_contact_ms = Some(now_ms);
        self.leader_id = Some(leader_id);
        self.last_leader_term = Some(term);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster_id() -> ClusterId {
        ClusterId(uuid::Uuid::nil())
    }

    fn three_node_voter_set() -> Vec<VoterInfo> {
        vec![
            VoterInfo { node_id: NodeId(1), endpoint: "n1".into() },
            VoterInfo { node_id: NodeId(2), endpoint: "n2".into() },
            VoterInfo { node_id: NodeId(3), endpoint: "n3".into() },
        ]
    }

    #[test]
    fn majority_of_three_is_two() {
        let state = NodeState::new(NodeId(1), test_cluster_id(), three_node_voter_set());
        assert_eq!(state.majority(), 2);
    }

    #[test]
    fn other_voters_excludes_self() {
        let state = NodeState::new(NodeId(1), test_cluster_id(), three_node_voter_set());
        let others = state.other_voters();
        assert_eq!(others.len(), 2);
        assert!(!others.contains(&NodeId(1)));
        assert!(others.contains(&NodeId(2)));
        assert!(others.contains(&NodeId(3)));
    }

    #[test]
    fn project_matches_internal_state() {
        let mut state = NodeState::new(NodeId(1), test_cluster_id(), three_node_voter_set());
        state.current_term = Term(5);
        state.role = Role::Leader;
        state.leader_id = Some(NodeId(1));
        state.high_watermark = Offset(42);

        let projected = state.project();
        assert_eq!(projected.current_term, Term(5));
        assert_eq!(projected.role, Role::Leader);
        assert_eq!(projected.leader_id, Some(NodeId(1)));
        assert_eq!(projected.high_watermark, Offset(42));
    }
}
