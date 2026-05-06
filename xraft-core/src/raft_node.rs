use crate::types::{NodeId, Term, Role, VoterInfo, AppRecord};
use crate::node_state::NodeState;
use crate::consensus_state::ConsensusState;
use crate::rpc::*;
use crate::config::RaftConfig;

/// A Raft node that manages consensus state.
/// Exposes the core consensus logic for use by the EventLoopDriver, which
/// adds the three-phase commit notification and IoAction production layer.
pub struct RaftNode {
    pub state: NodeState,
    pub config: RaftConfig,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(node_id: NodeId, voter_set: Vec<VoterInfo>, config: RaftConfig) -> Self {
        Self {
            state: NodeState::new(node_id, voter_set),
            config,
        }
    }

    /// Bootstrap the node as a follower.
    pub fn bootstrap(&mut self) {
        self.state.role = Role::Follower;
    }

    /// Get the current consensus state.
    pub fn read(&self) -> ConsensusState {
        self.state.to_consensus_state()
    }

    /// Propose a command (leader only).
    pub fn propose(&mut self, record: &AppRecord) -> Option<u64> {
        self.state.propose(record)
    }

    /// Start an election.
    pub fn start_election(&mut self, now_ms: u64) {
        let timeout = self.config.election_timeout_min_ms;
        self.state.start_election(now_ms, timeout);
    }

    /// Handle a vote request.
    pub fn handle_vote_request(&mut self, req: &VoteRequest, now_ms: u64) -> VoteResponse {
        self.state.handle_vote_request(req, now_ms, self.config.election_timeout_min_ms)
    }

    /// Handle a vote response. Returns true if we won the election.
    pub fn handle_vote_response(&mut self, resp: &VoteResponse, from: NodeId, now_ms: u64) -> bool {
        self.state.handle_vote_response(
            resp, from, now_ms,
            self.config.election_timeout_min_ms,
            self.config.election_timeout_max_ms,
        )
    }

    /// Handle a fetch request (leader side).
    pub fn handle_fetch_request(&mut self, req: &FetchRequest, now_ms: u64) -> FetchResponse {
        self.state.handle_fetch_request(req, now_ms)
    }

    /// Handle a fetch response (follower side).
    pub fn handle_fetch_response(&mut self, resp: &FetchResponse, now_ms: u64) -> usize {
        self.state.handle_fetch_response(resp, now_ms, self.config.election_timeout_min_ms)
    }

    /// Advance the high watermark (leader side).
    pub fn advance_high_watermark(&mut self) {
        self.state.advance_high_watermark();
    }

    /// Get node_id.
    pub fn node_id(&self) -> NodeId {
        self.state.node_id
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.state.role == Role::Leader
    }

    /// Get role.
    pub fn role(&self) -> Role {
        self.state.role
    }

    /// Get current term.
    pub fn term(&self) -> Term {
        self.state.current_term
    }

    /// Get high watermark.
    pub fn high_watermark(&self) -> u64 {
        self.state.high_watermark
    }

    /// Get log end offset.
    pub fn log_end_offset(&self) -> u64 {
        self.state.log_end_offset()
    }

    /// Get the log entries.
    pub fn log(&self) -> &[crate::log_entry::LogEntry] {
        &self.state.log
    }
}
