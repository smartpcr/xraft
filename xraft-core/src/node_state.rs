use crate::follower_progress::FollowerProgress;
use crate::log_entry::LogEntry;
use crate::types::{ClusterId, NodeId, Term};
use crate::voter::VoterInfo;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::time::Instant;

/// Node role in the Raft state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Initial state before bootstrap or recovery completes.
    Unattached,
    /// Following a known leader.
    Follower,
    /// Running an election.
    Candidate,
    /// Active leader of the cluster.
    Leader,
}

/// Pending membership change tracking (leader-only).
#[derive(Debug, Clone)]
pub struct PendingMembershipChange {
    pub offset: u64,
    pub voters: Vec<VoterInfo>,
}

/// Full internal protocol state (pub(crate)).
#[derive(Debug, Clone)]
pub struct NodeState {
    pub node_id: NodeId,
    pub cluster_id: ClusterId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,

    pub role: Role,
    pub leader_id: Option<NodeId>,

    // Log boundaries
    pub log_start_offset: u64,
    pub log_end_offset: u64,
    /// Exclusive upper bound: entries with offset < HW are committed.
    pub high_watermark: u64,

    /// In-memory log entries. Used for commit processing and replication.
    /// On a real node, this would be backed by LogStore; we maintain an
    /// in-memory view for the event loop's synchronous processing needs.
    pub log: Vec<LogEntry>,

    // Voter set (from latest committed VotersRecord or snapshot)
    pub voter_set: Vec<VoterInfo>,
    pub observers: HashSet<NodeId>,

    // Pending membership change (leader-only, at most one)
    pub pending_membership_change: Option<PendingMembershipChange>,

    // Leader-only state
    pub follower_state: HashMap<NodeId, FollowerProgress>,

    // Election state
    pub election_deadline: Instant,
    pub votes_received: HashSet<NodeId>,
    pub pre_votes_received: HashSet<NodeId>,
    pub check_quorum_deadline: Instant,

    /// Deadline for the next periodic Fetch RPC (follower-only).
    pub fetch_deadline: Instant,

    /// Leader-epoch checkpoint: maps epoch (term) → start_offset of that
    /// leader's tenure. Updated when a LeaderChangeMessage is committed.
    /// Used for Fetch divergence detection (architecture §3.2, §5.3).
    pub leader_epoch_checkpoint: BTreeMap<Term, u64>,
}

impl NodeState {
    /// Create a new NodeState in Follower role with the given identity.
    pub fn new(node_id: NodeId, cluster_id: ClusterId) -> Self {
        let now = Instant::now();
        NodeState {
            node_id,
            cluster_id,
            current_term: Term(0),
            voted_for: None,
            role: Role::Follower,
            leader_id: None,
            log_start_offset: 0,
            log_end_offset: 0,
            high_watermark: 0,
            log: Vec::new(),
            voter_set: Vec::new(),
            observers: HashSet::new(),
            pending_membership_change: None,
            follower_state: HashMap::new(),
            election_deadline: now,
            votes_received: HashSet::new(),
            pre_votes_received: HashSet::new(),
            check_quorum_deadline: now,
            fetch_deadline: now,
            leader_epoch_checkpoint: BTreeMap::new(),
        }
    }

    /// Return the term of the last log entry, or Term(0) if log is empty.
    pub fn last_log_term(&self) -> Term {
        self.log.last().map_or(Term(0), |e| e.term)
    }

    /// Get entries in range [start, end) from the in-memory log.
    pub fn entries_in_range(&self, start: u64, end: u64) -> Vec<LogEntry> {
        self.log
            .iter()
            .filter(|e| e.offset >= start && e.offset < end)
            .cloned()
            .collect()
    }

    /// Append entries to the in-memory log and advance log_end_offset.
    pub fn append_entries(&mut self, entries: &[LogEntry]) {
        for entry in entries {
            assert_eq!(
                entry.offset, self.log_end_offset,
                "entry offset {} does not match log_end_offset {}",
                entry.offset, self.log_end_offset
            );
            self.log.push(entry.clone());
            self.log_end_offset = entry.offset + 1;
        }
    }

    /// Truncate the log from the given offset (inclusive) and update log_end_offset.
    ///
    /// If `from_offset` exceeds the current `log_end_offset`, the call is a
    /// no-op — we never move `log_end_offset` forward, which would create
    /// holes in the log.
    pub fn truncate_suffix(&mut self, from_offset: u64) {
        if from_offset >= self.log_end_offset {
            // Nothing to truncate — `from_offset` is at or beyond the log end.
            return;
        }
        self.log.retain(|e| e.offset < from_offset);
        self.log_end_offset = from_offset;
    }
}

/// Public projected type returned by RaftNode::read().
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

impl From<&NodeState> for ConsensusState {
    fn from(state: &NodeState) -> Self {
        ConsensusState {
            node_id: state.node_id,
            current_term: state.current_term,
            role: state.role,
            leader_id: state.leader_id,
            log_end_offset: state.log_end_offset,
            high_watermark: state.high_watermark,
            voter_set: state.voter_set.clone(),
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
