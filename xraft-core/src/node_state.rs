use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::Instant;

use crate::consensus_state::{ConsensusState, Role};
use crate::follower_progress::FollowerProgress;
use crate::types::{ClusterId, NodeId, Term};
use crate::voter::VoterInfo;

/// Pending membership change tracking (leader-only, at most one).
#[derive(Debug, Clone)]
pub struct PendingMembershipChange {
    pub offset: u64,
    pub voters: Vec<VoterInfo>,
}

/// Full internal protocol state. The public projection is `ConsensusState`.
#[derive(Debug)]
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
    pub high_watermark: u64,
    /// Term of the last log entry. Updated on append, truncation, and snapshot restore.
    /// When the log is empty, falls back to the snapshot's `last_included_term` (or Term(0)).
    pub last_log_term: Term,

    // Voter set (from latest committed VotersRecord or snapshot)
    pub voter_set: Vec<VoterInfo>,
    pub observers: HashSet<NodeId>,

    // Pending membership change (leader-only)
    pub pending_membership_change: Option<PendingMembershipChange>,

    // Leader-only: per-follower progress
    pub follower_state: HashMap<NodeId, FollowerProgress>,

    // Election state
    pub election_deadline: Instant,
    pub votes_received: HashSet<NodeId>,
    pub pre_votes_received: HashSet<NodeId>,
    pub check_quorum_deadline: Instant,
    /// The actual randomized election-timeout interval used for check-quorum.
    /// Set when transitioning to Leader; used as both the timer period and
    /// the freshness cutoff for follower Fetch timestamps.
    pub check_quorum_interval: Duration,
}

impl NodeState {
    /// Create a new NodeState with default initial values.
    pub fn new(
        node_id: NodeId,
        cluster_id: ClusterId,
        voter_set: Vec<VoterInfo>,
        now: Instant,
    ) -> Self {
        Self {
            node_id,
            cluster_id,
            current_term: Term(0),
            voted_for: None,
            role: Role::Unattached,
            leader_id: None,
            log_start_offset: 0,
            log_end_offset: 0,
            high_watermark: 0,
            last_log_term: Term(0),
            voter_set,
            observers: HashSet::new(),
            pending_membership_change: None,
            follower_state: HashMap::new(),
            election_deadline: now,
            votes_received: HashSet::new(),
            pre_votes_received: HashSet::new(),
            check_quorum_deadline: now,
            check_quorum_interval: Duration::ZERO,
        }
    }

    /// Produce the public projection (7 fields).
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

    /// Returns the number of voters (including self).
    pub fn voter_count(&self) -> usize {
        self.voter_set.len()
    }

    /// Returns the majority quorum size for the current voter set.
    pub fn majority(&self) -> usize {
        self.voter_count() / 2 + 1
    }

    /// Check if this node is a voter.
    pub fn is_voter(&self) -> bool {
        self.voter_set.iter().any(|v| v.node_id == self.node_id)
    }

    /// Transition to follower for the given term, clearing election state.
    pub fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>, deadline: Instant) {
        self.current_term = term;
        self.role = Role::Follower;
        self.leader_id = leader_id;
        self.voted_for = None;
        self.votes_received.clear();
        self.pre_votes_received.clear();
        self.follower_state.clear();
        self.election_deadline = deadline;
    }

    /// Transition to candidate: increment term, vote for self.
    pub fn become_candidate(&mut self, deadline: Instant) {
        self.current_term = self.current_term.next();
        self.role = Role::Candidate;
        self.leader_id = None;
        self.voted_for = Some(self.node_id);
        self.votes_received.clear();
        self.votes_received.insert(self.node_id);
        self.pre_votes_received.clear();
        self.follower_state.clear();
        self.election_deadline = deadline;
    }

    /// Transition to leader: initialize follower progress, set check quorum deadline.
    /// Follower timestamps start as `None` — a Fetch must actually arrive before
    /// a follower counts toward the quorum check.
    ///
    /// `check_quorum_interval` is the actual randomized election-timeout duration,
    /// used as both the timer period and the freshness cutoff window.
    ///
    /// **Safety**: `voted_for` is preserved as `Some(self.node_id)` so that
    /// same-term VoteRequests are correctly rejected (at most one leader per term).
    pub fn become_leader(&mut self, now: Instant, check_quorum_interval: Duration) {
        self.role = Role::Leader;
        self.leader_id = Some(self.node_id);
        // Keep voted_for = Some(self) — clearing it would allow granting a
        // same-term vote to another candidate, violating leader uniqueness.
        debug_assert_eq!(self.voted_for, Some(self.node_id),
            "become_leader expects voted_for == self from candidate phase");
        self.votes_received.clear();
        self.pre_votes_received.clear();

        // Initialize follower progress for all voters except self.
        // last_fetch_timestamp is None — no follower has actually fetched yet.
        self.follower_state.clear();
        for voter in &self.voter_set {
            if voter.node_id != self.node_id {
                self.follower_state.insert(
                    voter.node_id,
                    FollowerProgress {
                        node_id: voter.node_id,
                        fetch_offset: 0,
                        last_fetch_timestamp: None,
                        is_voter: true,
                    },
                );
            }
        }

        // Set check quorum deadline and interval using the actual election timeout
        self.check_quorum_interval = check_quorum_interval;
        self.check_quorum_deadline = now + check_quorum_interval;
    }

    /// Check if a majority of voters have fetched within the timeout window.
    /// The leader counts itself as always having "fetched" (it's the leader).
    /// A follower with `last_fetch_timestamp == None` has never fetched and
    /// does NOT count toward quorum.
    /// Returns true if quorum is maintained.
    pub fn check_quorum(&self, now: Instant, election_timeout: std::time::Duration) -> bool {
        if self.role != Role::Leader {
            return false;
        }

        let cutoff = now - election_timeout;
        // Count the leader itself
        let mut live_voters = 1usize;

        for voter_info in &self.voter_set {
            if voter_info.node_id == self.node_id {
                continue;
            }
            if let Some(progress) = self.follower_state.get(&voter_info.node_id) {
                if progress.is_voter {
                    if let Some(ts) = progress.last_fetch_timestamp {
                        if ts >= cutoff {
                            live_voters += 1;
                        }
                    }
                }
            }
        }

        live_voters >= self.majority()
    }

    /// Update a follower's fetch timestamp (called when leader receives FetchRequest).
    pub fn record_fetch(&mut self, node_id: NodeId, fetch_offset: u64, now: Instant) {
        if let Some(progress) = self.follower_state.get_mut(&node_id) {
            progress.fetch_offset = fetch_offset;
            progress.last_fetch_timestamp = Some(now);
        }
    }

    /// Check if a given node is in the current voter set.
    pub fn is_in_voter_set(&self, node_id: NodeId) -> bool {
        self.voter_set.iter().any(|v| v.node_id == node_id)
    }

    /// Update last_log_term when entries are appended to the log.
    pub fn update_last_log_term(&mut self, term: Term) {
        self.last_log_term = term;
    }
}
