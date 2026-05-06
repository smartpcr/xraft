use std::collections::HashSet;
use std::time::Instant;

use crate::config::RaftConfig;
use crate::consensus_state::Role;
use crate::io_action::{IoAction, IoActionBatch};
use crate::quorum_state::QuorumState;
use crate::rpc::{RpcEnvelope, RpcPayload, VoteRequest, VoteResponse};
use crate::traits::Clock;
use crate::types::{ClusterId, NodeId, Term};
use crate::voter::VoterInfo;

/// State-transition events emitted by the election manager.
///
/// These are *not* I/O actions — they notify the EventLoop of role changes
/// so it can update `NodeState`, adjust timers, and trigger downstream logic
/// (e.g., append a `LeaderChangeMessage` on becoming leader).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionEvent {
    /// The node transitioned to Leader for the given term.
    BecameLeader { term: Term },
    /// The node stepped down to Follower (higher term seen).
    SteppedDown { new_term: Term },
    /// A new election was started (node became Candidate).
    ElectionStarted { term: Term },
}

/// Alias retained for backward compatibility with lib.rs re-exports.
pub type ElectionAction = ElectionEvent;

/// Output from an ElectionManager method.
///
/// Separates I/O into two phases to enforce fsync-before-ack:
/// - `persist_first`: `IoAction::PersistQuorumState` — must be fsynced before
///   any RPC is sent.
/// - `then_send`: `IoAction::SendRpc` — dispatched only after persistence
///   completes.
/// - `events`: state-transition notifications for the EventLoop.
///
/// The `IoStage::execute_election_output` method enforces this contract:
/// it awaits all `persist_first` actions before dispatching `then_send`.
/// If persistence fails, no RPCs are sent.
#[derive(Debug, Clone)]
pub struct ElectionOutput {
    /// Persistence actions that MUST complete (fsync) before any RPCs.
    pub persist_first: IoActionBatch,
    /// RPC send actions dispatched after persistence completes.
    pub then_send: IoActionBatch,
    /// State-transition events (not I/O).
    pub events: Vec<ElectionEvent>,
}

impl ElectionOutput {
    fn empty() -> Self {
        Self {
            persist_first: IoActionBatch::new(),
            then_send: IoActionBatch::new(),
            events: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.persist_first.is_empty() && self.then_send.is_empty() && self.events.is_empty()
    }
}

/// Manages the election protocol for a single Raft node.
///
/// Pure protocol logic — no I/O, no async. Methods return [`ElectionOutput`]
/// containing [`IoAction`] values that the EventLoop dispatches through the
/// `IoStage`, plus [`ElectionEvent`]s for state-transition notifications.
///
/// Timer management uses the [`Clock`] trait: the EventLoop passes `&dyn Clock`
/// to [`tick`](Self::tick) and [`reset_election_deadline`](Self::reset_election_deadline).
/// The `ElectionManager` tracks a randomized `election_deadline` internally.
///
/// # Term-update ordering
///
/// Every handler checks the incoming message's term **first**. If the message
/// carries a higher term, the node steps down to Follower and clears `voted_for`
/// before any other processing (pre-vote rejection, membership check, source
/// validation). This ensures that even messages that are ultimately rejected
/// (pre-vote, non-voter, source mismatch) still force the term-update rule.
pub struct ElectionManager {
    node_id: NodeId,
    cluster_id: ClusterId,
    current_term: Term,
    voted_for: Option<NodeId>,
    role: Role,
    voter_set: Vec<VoterInfo>,
    votes_received: HashSet<NodeId>,
    last_log_term: Term,
    last_log_offset: u64,
    /// Deadline at which the node should start an election (Follower/Candidate).
    /// `None` when the node is Leader (leaders do not time out).
    election_deadline: Option<Instant>,
    config: RaftConfig,
}

impl ElectionManager {
    pub fn new(
        node_id: NodeId,
        cluster_id: ClusterId,
        voter_set: Vec<VoterInfo>,
        config: RaftConfig,
    ) -> Self {
        Self {
            node_id,
            cluster_id,
            current_term: Term(0),
            voted_for: None,
            role: Role::Follower,
            voter_set,
            votes_received: HashSet::new(),
            last_log_term: Term(0),
            last_log_offset: 0,
            election_deadline: None,
            config,
        }
    }

    // ── Accessors ──────────────────────────────────────────────────────

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn votes_received(&self) -> &HashSet<NodeId> {
        &self.votes_received
    }

    pub fn election_deadline(&self) -> Option<Instant> {
        self.election_deadline
    }

    pub fn voter_set(&self) -> &[VoterInfo] {
        &self.voter_set
    }

    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    pub fn set_log_state(&mut self, last_log_term: Term, last_log_offset: u64) {
        self.last_log_term = last_log_term;
        self.last_log_offset = last_log_offset;
    }

    /// Restore persisted state (used on startup).
    pub fn restore_state(&mut self, term: Term, voted_for: Option<NodeId>) {
        self.current_term = term;
        self.voted_for = voted_for;
    }

    // ── Deadline management ────────────────────────────────────────────

    /// Set (or reset) the election deadline using a randomized timeout from the Clock.
    ///
    /// Called by the EventLoop:
    /// - On startup / follower initialization
    /// - After granting a vote
    /// - After stepping down to Follower
    /// - After receiving valid leader traffic (e.g. Fetch response)
    pub fn reset_election_deadline(&mut self, clock: &dyn Clock) {
        let timeout = clock.random_election_timeout();
        self.election_deadline = Some(clock.now() + timeout);
    }

    /// Clear the election deadline (used when becoming Leader).
    fn clear_election_deadline(&mut self) {
        self.election_deadline = None;
    }

    /// Tick the election manager: checks whether the election deadline has expired.
    ///
    /// Called by the EventLoop on each iteration. If the deadline has passed and the
    /// node is a Follower or Candidate, starts an election. Returns an empty output
    /// if no action is needed.
    pub fn tick(&mut self, clock: &dyn Clock) -> ElectionOutput {
        if let Some(deadline) = self.election_deadline {
            if clock.now() >= deadline {
                return self.on_election_timeout(clock);
            }
        }
        ElectionOutput::empty()
    }

    // ── Internal helpers ───────────────────────────────────────────────

    fn majority_size(&self) -> usize {
        self.voter_set.len() / 2 + 1
    }

    fn is_voter(&self, node_id: NodeId) -> bool {
        self.voter_set.iter().any(|v| v.node_id == node_id)
    }

    /// Build a `PersistQuorumState` IoAction from current state.
    fn persist_action(&self) -> IoAction {
        IoAction::PersistQuorumState(QuorumState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            leader_id: None,
            leader_epoch: 0,
        })
    }

    /// Wrap a VoteRequest as an RpcEnvelope targeted at `target`.
    fn vote_request_envelope(&self, request: &VoteRequest, target: NodeId) -> RpcEnvelope {
        let _ = target; // used for future routing metadata
        RpcEnvelope {
            cluster_id: self.cluster_id,
            leader_epoch: 0, // not leader-scoped during elections
            source: self.node_id,
            payload: RpcPayload::VoteRequest(request.clone()),
        }
    }

    /// Wrap a VoteResponse as an RpcEnvelope targeted at `target`.
    fn vote_response_envelope(&self, response: &VoteResponse, target: NodeId) -> RpcEnvelope {
        let _ = target;
        RpcEnvelope {
            cluster_id: self.cluster_id,
            leader_epoch: 0,
            source: self.node_id,
            payload: RpcPayload::VoteResponse(response.clone()),
        }
    }

    /// Create SendRpc IoActions for broadcasting a VoteRequest to all voter peers.
    fn broadcast_vote_request_actions(&self, request: &VoteRequest) -> Vec<IoAction> {
        self.voter_set
            .iter()
            .filter(|v| v.node_id != self.node_id)
            .map(|v| {
                IoAction::SendRpc(
                    v.node_id,
                    self.vote_request_envelope(request, v.node_id),
                )
            })
            .collect()
    }

    // ── Election timeout ───────────────────────────────────────────────

    /// Called when the election timeout expires.
    ///
    /// Only valid when the node is a Follower or Candidate. Leaders do not
    /// start elections. Returns an empty output if called in an invalid role.
    ///
    /// Transitions to Candidate, increments term, votes for self, sets a new
    /// randomized election deadline, and emits:
    ///   1. `PersistQuorumState` (persist_first) — must fsync before RPCs
    ///   2. `SendRpc` per peer (then_send) — targeted vote requests
    ///   3. (optionally) `BecameLeader` event — if single-node cluster
    pub fn on_election_timeout(&mut self, clock: &dyn Clock) -> ElectionOutput {
        if self.role == Role::Leader {
            return ElectionOutput::empty();
        }

        self.current_term = Term(self.current_term.0 + 1);
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id);
        self.votes_received.clear();
        self.votes_received.insert(self.node_id);

        // Set a new randomized deadline for this election attempt
        self.reset_election_deadline(clock);

        let vote_request = VoteRequest {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_offset: self.last_log_offset,
            last_log_term: self.last_log_term,
            is_pre_vote: false,
        };

        let mut output = ElectionOutput::empty();

        // Persist MUST happen before any RPC send (fsync-before-ack)
        output.persist_first.push(self.persist_action());

        // Broadcast targeted vote requests to each peer
        for action in self.broadcast_vote_request_actions(&vote_request) {
            output.then_send.push(action);
        }

        output.events.push(ElectionEvent::ElectionStarted {
            term: self.current_term,
        });

        // Single-node cluster: already have majority
        if self.votes_received.len() >= self.majority_size() {
            self.role = Role::Leader;
            self.clear_election_deadline();
            output.events.push(ElectionEvent::BecameLeader {
                term: self.current_term,
            });
        }

        output
    }

    // ── Vote request handling ──────────────────────────────────────────

    /// Handle an incoming VoteRequest from another node.
    ///
    /// **Term-update ordering**: higher-term check is FIRST. Any VoteRequest
    /// with a higher term causes immediate step-down before pre-vote, membership,
    /// or source-validation checks. This ensures the Raft term-update invariant
    /// is never bypassed.
    ///
    /// After term update, the remaining rules apply:
    /// (a) pre-vote requests are rejected (Stage 4.3 handles pre-vote)
    /// (b) candidate must be a known voter (membership check)
    /// (c) envelope source must match candidate_id
    /// (d) requester term ≥ current term (stale rejection)
    /// (e) not already voted in this term or voted for same candidate
    /// (f) requester log is at least as up-to-date (last log term/offset)
    ///
    /// Every response path that follows a state mutation includes
    /// `PersistQuorumState` in `persist_first` (fsync-before-ack).
    pub fn handle_vote_request(
        &mut self,
        from: NodeId,
        request: &VoteRequest,
        clock: &dyn Clock,
    ) -> ElectionOutput {
        let mut output = ElectionOutput::empty();
        let mut state_dirty = false;

        // ── FIRST: Term update rule ──
        // Any message with a higher term causes immediate step-down,
        // even if the message will ultimately be rejected.
        if request.term.0 > self.current_term.0 {
            self.step_down(request.term);
            self.reset_election_deadline(clock);
            state_dirty = true;
            output.events.push(ElectionEvent::SteppedDown {
                new_term: request.term,
            });
        }

        // ── Pre-vote rejection ──
        // Pre-vote is handled by Stage 4.3; reject here.
        // If we stepped down above, persist before sending.
        if request.is_pre_vote {
            let response = VoteResponse {
                term: self.current_term,
                vote_granted: false,
                is_pre_vote: true,
            };
            if state_dirty {
                output.persist_first.push(self.persist_action());
            }
            output.then_send.push(IoAction::SendRpc(
                from,
                self.vote_response_envelope(&response, from),
            ));
            return output;
        }

        // ── Membership check ──
        // Reject candidates not in the voter set (observers/unknown nodes).
        if !self.is_voter(request.candidate_id) {
            let response = VoteResponse {
                term: self.current_term,
                vote_granted: false,
                is_pre_vote: false,
            };
            if state_dirty {
                output.persist_first.push(self.persist_action());
            }
            output.then_send.push(IoAction::SendRpc(
                from,
                self.vote_response_envelope(&response, from),
            ));
            return output;
        }

        // ── Source validation ──
        // Reject if envelope source doesn't match candidate_id.
        if from != request.candidate_id {
            let response = VoteResponse {
                term: self.current_term,
                vote_granted: false,
                is_pre_vote: false,
            };
            if state_dirty {
                output.persist_first.push(self.persist_action());
            }
            output.then_send.push(IoAction::SendRpc(
                from,
                self.vote_response_envelope(&response, from),
            ));
            return output;
        }

        // ── Stale term rejection ──
        if request.term.0 < self.current_term.0 {
            let response = VoteResponse {
                term: self.current_term,
                vote_granted: false,
                is_pre_vote: false,
            };
            // State may have changed via step_down above (unlikely here since
            // we'd have stepped *up* to request.term, making this unreachable,
            // but we persist defensively).
            output.persist_first.push(self.persist_action());
            output.then_send.push(IoAction::SendRpc(
                from,
                self.vote_response_envelope(&response, from),
            ));
            return output;
        }

        // ── Vote eligibility ──
        let can_vote = match self.voted_for {
            None => true,
            Some(id) => id == request.candidate_id,
        };

        // ── Log freshness check ──
        let log_ok = self.is_log_up_to_date(request.last_log_term, request.last_log_offset);

        let vote_granted = can_vote && log_ok;

        if vote_granted {
            self.voted_for = Some(request.candidate_id);
            // Granting a vote implies recognising the candidate; step down if Candidate
            if self.role == Role::Candidate {
                self.role = Role::Follower;
            }
            // Reset election deadline when granting a vote
            self.reset_election_deadline(clock);
        }

        let response = VoteResponse {
            term: self.current_term,
            vote_granted,
            is_pre_vote: false,
        };

        // fsync-before-ack: always persist before sending response
        output.persist_first.push(self.persist_action());
        output.then_send.push(IoAction::SendRpc(
            from,
            self.vote_response_envelope(&response, from),
        ));

        output
    }

    // ── Vote response handling ─────────────────────────────────────────

    /// Handle an incoming VoteResponse.
    ///
    /// **Term-update ordering**: higher-term check is FIRST. Even pre-vote
    /// responses with higher terms cause step-down.
    ///
    /// Pre-vote responses (`is_pre_vote == true`) are ignored for vote
    /// counting — Stage 4.3 handles pre-vote protocol separately.
    ///
    /// Collects votes from known voters only; transitions to Leader when a
    /// majority is reached. Clears election deadline on becoming Leader.
    pub fn handle_vote_response(
        &mut self,
        from: NodeId,
        response: &VoteResponse,
        clock: &dyn Clock,
    ) -> ElectionOutput {
        let mut output = ElectionOutput::empty();

        // ── FIRST: Term update rule ──
        if response.term.0 > self.current_term.0 {
            self.step_down(response.term);
            self.reset_election_deadline(clock);
            output.persist_first.push(self.persist_action());
            output.events.push(ElectionEvent::SteppedDown {
                new_term: response.term,
            });
            return output;
        }

        // ── Ignore pre-vote responses ──
        // Pre-vote protocol is handled by Stage 4.3. Pre-vote responses
        // must NOT be counted toward real election vote tallies.
        if response.is_pre_vote {
            return output;
        }

        // Ignore stale or irrelevant responses
        if response.term != self.current_term || self.role != Role::Candidate {
            return output;
        }

        // Only count votes from known voters
        if response.vote_granted && self.is_voter(from) {
            self.votes_received.insert(from);

            if self.votes_received.len() >= self.majority_size() {
                self.role = Role::Leader;
                self.clear_election_deadline();
                output.events.push(ElectionEvent::BecameLeader {
                    term: self.current_term,
                });
            }
        }

        output
    }

    // ── Term step-down ─────────────────────────────────────────────────

    /// Apply the term-update rule: any message with a higher term causes
    /// immediate step-down to Follower and clears voted_for.
    pub fn step_down(&mut self, new_term: Term) {
        self.current_term = new_term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes_received.clear();
    }

    /// Check if a candidate's log is at least as up-to-date as ours.
    ///
    /// Raft §5.4.1: Compare last log term first; if equal, compare offset.
    fn is_log_up_to_date(&self, candidate_last_term: Term, candidate_last_offset: u64) -> bool {
        if candidate_last_term.0 != self.last_log_term.0 {
            return candidate_last_term.0 > self.last_log_term.0;
        }
        candidate_last_offset >= self.last_log_offset
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_action::IoAction;
    use crate::rpc::RpcPayload;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    // ── Inline SimulatedClock with randomized deadlines ──────────────

    struct SimulatedClock {
        current: Arc<Mutex<Instant>>,
        min_timeout_ms: Cell<u64>,
        max_timeout_ms: Cell<u64>,
        /// Monotonic counter used for deterministic "randomization"
        /// within [min, max]. Each call to random_election_timeout
        /// returns a different value by stepping through the range.
        call_counter: Cell<u64>,
    }

    impl SimulatedClock {
        fn new() -> Self {
            Self {
                current: Arc::new(Mutex::new(Instant::now())),
                min_timeout_ms: Cell::new(150),
                max_timeout_ms: Cell::new(300),
                call_counter: Cell::new(0),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut current = self.current.lock().unwrap();
            *current += duration;
        }

        /// For tests that need a fixed timeout, set min == max.
        fn set_election_timeout(&self, timeout: Duration) {
            let ms = timeout.as_millis() as u64;
            self.min_timeout_ms.set(ms);
            self.max_timeout_ms.set(ms);
        }

        fn set_timeout_range(&self, min_ms: u64, max_ms: u64) {
            self.min_timeout_ms.set(min_ms);
            self.max_timeout_ms.set(max_ms);
        }
    }

    impl Clock for SimulatedClock {
        fn now(&self) -> Instant {
            *self.current.lock().unwrap()
        }

        fn random_election_timeout(&self) -> Duration {
            let min = self.min_timeout_ms.get();
            let max = self.max_timeout_ms.get();
            let range = max - min;
            if range == 0 {
                return Duration::from_millis(min);
            }
            let counter = self.call_counter.get();
            self.call_counter.set(counter + 1);
            // Deterministic spread: step through range using golden ratio
            let offset = ((counter * 618033) % (range + 1)).min(range);
            Duration::from_millis(min + offset)
        }
    }

    // ── Inline MemoryQuorumStateStore ──────────────────────────────────

    struct MemoryQuorumStateStore {
        state: Mutex<Option<QuorumState>>,
    }

    impl MemoryQuorumStateStore {
        fn new() -> Self {
            Self {
                state: Mutex::new(None),
            }
        }

        fn last_saved(&self) -> Option<QuorumState> {
            self.state.lock().unwrap().clone()
        }

        fn save_sync(&self, qs: &QuorumState) {
            *self.state.lock().unwrap() = Some(qs.clone());
        }
    }

    // ── Test helpers ──────────────────────────────────────────────────

    fn test_cluster_id() -> ClusterId {
        ClusterId(uuid::Uuid::nil())
    }

    fn three_node_voter_set() -> Vec<VoterInfo> {
        vec![
            VoterInfo { node_id: NodeId(1), endpoint: "127.0.0.1:9001".into() },
            VoterInfo { node_id: NodeId(2), endpoint: "127.0.0.1:9002".into() },
            VoterInfo { node_id: NodeId(3), endpoint: "127.0.0.1:9003".into() },
        ]
    }

    fn make_manager(node_id: u64) -> ElectionManager {
        ElectionManager::new(
            NodeId(node_id),
            test_cluster_id(),
            three_node_voter_set(),
            RaftConfig::default(),
        )
    }

    /// Extract the VoteRequest from SendRpc actions.
    fn extract_vote_request_from_sends(output: &ElectionOutput) -> VoteRequest {
        for action in &output.then_send.actions {
            if let IoAction::SendRpc(_, envelope) = action {
                if let RpcPayload::VoteRequest(req) = &envelope.payload {
                    return req.clone();
                }
            }
        }
        panic!("expected SendRpc with VoteRequest in then_send");
    }

    /// Extract all SendRpc targets for VoteRequests.
    fn extract_vote_request_targets(output: &ElectionOutput) -> Vec<NodeId> {
        output.then_send.actions.iter().filter_map(|a| {
            if let IoAction::SendRpc(target, envelope) = a {
                if matches!(envelope.payload, RpcPayload::VoteRequest(_)) {
                    return Some(*target);
                }
            }
            None
        }).collect()
    }

    /// Extract the VoteResponse from a SendRpc action.
    fn extract_vote_response_from_output(output: &ElectionOutput) -> (NodeId, VoteResponse) {
        for action in &output.then_send.actions {
            if let IoAction::SendRpc(target, envelope) = action {
                if let RpcPayload::VoteResponse(resp) = &envelope.payload {
                    return (*target, resp.clone());
                }
            }
        }
        panic!("expected SendRpc with VoteResponse in then_send");
    }

    /// Assert that persist_first has at least one PersistQuorumState action.
    fn assert_has_persist(output: &ElectionOutput) -> QuorumState {
        for action in &output.persist_first.actions {
            if let IoAction::PersistQuorumState(qs) = action {
                return qs.clone();
            }
        }
        panic!("expected PersistQuorumState in persist_first");
    }

    /// Execute PersistQuorumState actions from an ElectionOutput into a MemoryQuorumStateStore.
    fn execute_persists(output: &ElectionOutput, store: &MemoryQuorumStateStore) {
        for action in &output.persist_first.actions {
            if let IoAction::PersistQuorumState(qs) = action {
                store.save_sync(qs);
            }
        }
    }

    /// Assert that all SendRpc envelopes have the correct cluster_id and source.
    fn assert_envelope_fields(output: &ElectionOutput, expected_source: NodeId, expected_cluster_id: &ClusterId) {
        for action in output.then_send.actions.iter().chain(output.persist_first.actions.iter()) {
            if let IoAction::SendRpc(_, envelope) = action {
                assert_eq!(envelope.source, expected_source, "envelope source mismatch");
                assert_eq!(envelope.cluster_id, *expected_cluster_id, "envelope cluster_id mismatch");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3-node cluster test harness with simulated clock and message routing
    // ═══════════════════════════════════════════════════════════════════

    struct ClusterHarness {
        nodes: HashMap<NodeId, ElectionManager>,
        stores: HashMap<NodeId, MemoryQuorumStateStore>,
        clock: SimulatedClock,
    }

    impl ClusterHarness {
        fn new() -> Self {
            let voter_set = three_node_voter_set();
            let cluster_id = test_cluster_id();
            let clock = SimulatedClock::new();
            // Use a fixed timeout for deterministic cluster tests
            clock.set_election_timeout(Duration::from_millis(200));
            let mut nodes = HashMap::new();
            let mut stores = HashMap::new();
            for id in 1..=3u64 {
                let nid = NodeId(id);
                let mgr = ElectionManager::new(
                    nid, cluster_id, voter_set.clone(), RaftConfig::default(),
                );
                nodes.insert(nid, mgr);
                stores.insert(nid, MemoryQuorumStateStore::new());
            }
            Self {
                nodes,
                stores,
                clock,
            }
        }

        /// Initialize all nodes' election deadlines using the shared clock.
        fn init_deadlines(&mut self) {
            for (_, node) in self.nodes.iter_mut() {
                node.reset_election_deadline(&self.clock);
            }
        }

        /// Tick a specific node to check if its election deadline has expired.
        fn tick_node(&mut self, node_id: NodeId) -> ElectionOutput {
            let node = self.nodes.get_mut(&node_id).unwrap();
            let output = node.tick(&self.clock);
            execute_persists(&output, self.stores.get(&node_id).unwrap());
            output
        }

        /// Trigger election timeout explicitly on a node.
        fn election_timeout(&mut self, node_id: NodeId) -> ElectionOutput {
            let node = self.nodes.get_mut(&node_id).unwrap();
            let output = node.on_election_timeout(&self.clock);
            execute_persists(&output, self.stores.get(&node_id).unwrap());
            output
        }

        /// Deliver a VoteRequest from `from` to `to`.
        fn deliver_vote_request(
            &mut self,
            from: NodeId,
            to: NodeId,
            request: &VoteRequest,
        ) -> ElectionOutput {
            let node = self.nodes.get_mut(&to).unwrap();
            let output = node.handle_vote_request(from, request, &self.clock);
            execute_persists(&output, self.stores.get(&to).unwrap());
            output
        }

        /// Deliver a VoteResponse from `from` to `to`.
        fn deliver_vote_response(
            &mut self,
            from: NodeId,
            to: NodeId,
            response: &VoteResponse,
        ) -> ElectionOutput {
            let node = self.nodes.get_mut(&to).unwrap();
            let output = node.handle_vote_response(from, response, &self.clock);
            execute_persists(&output, self.stores.get(&to).unwrap());
            output
        }

        fn node(&self, id: NodeId) -> &ElectionManager {
            self.nodes.get(&id).unwrap()
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Scenario: Successful election — 3-node cluster, clock-driven
    //
    // Given a 3-node cluster with simulated clock,
    // When N1's election timeout expires and N2, N3 grant votes,
    // Then N1 becomes Leader for the new term.
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_successful_election_3_node_flow() {
        let mut cluster = ClusterHarness::new();
        cluster.init_deadlines();

        // Before deadline: tick produces no output
        let no_output = cluster.tick_node(NodeId(1));
        assert!(no_output.is_empty());
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Follower);

        // Advance clock past election timeout (default 200ms)
        cluster.clock.advance(Duration::from_millis(250));

        // N1's tick fires the election timeout
        let n1_output = cluster.tick_node(NodeId(1));
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Candidate);
        assert_eq!(cluster.node(NodeId(1)).current_term(), Term(1));

        // Verify persistence before sends
        let persisted = assert_has_persist(&n1_output);
        assert_eq!(persisted.current_term, Term(1));
        assert_eq!(persisted.voted_for, Some(NodeId(1)));

        // Verify targeted SendRpc actions (not payload-only broadcast)
        let targets = extract_vote_request_targets(&n1_output);
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&NodeId(2)));
        assert!(targets.contains(&NodeId(3)));
        // Self is NOT in the broadcast targets
        assert!(!targets.contains(&NodeId(1)));

        // Verify envelope fields on all outbound RPCs
        assert_envelope_fields(&n1_output, NodeId(1), &test_cluster_id());

        // Verify ElectionStarted event
        assert!(n1_output.events.iter().any(|e|
            matches!(e, ElectionEvent::ElectionStarted { term } if *term == Term(1))
        ));

        // Verify a new election deadline was set (for candidate retry)
        assert!(cluster.node(NodeId(1)).election_deadline().is_some());

        let vote_req = extract_vote_request_from_sends(&n1_output);
        assert_eq!(vote_req.term, Term(1));
        assert_eq!(vote_req.candidate_id, NodeId(1));

        // ── N2 grants vote ──
        let n2_output = cluster.deliver_vote_request(NodeId(1), NodeId(2), &vote_req);
        let n2_persisted = assert_has_persist(&n2_output);
        assert_eq!(n2_persisted.current_term, Term(1));
        assert_eq!(n2_persisted.voted_for, Some(NodeId(1)));
        let (target, resp_n2) = extract_vote_response_from_output(&n2_output);
        assert_eq!(target, NodeId(1));
        assert!(resp_n2.vote_granted);
        assert_eq!(resp_n2.term, Term(1));
        // Verify envelope on response
        assert_envelope_fields(&n2_output, NodeId(2), &test_cluster_id());

        // ── N3 grants vote ──
        let n3_output = cluster.deliver_vote_request(NodeId(1), NodeId(3), &vote_req);
        let (_, resp_n3) = extract_vote_response_from_output(&n3_output);
        assert!(resp_n3.vote_granted);

        // ── Deliver N2's response to N1 → majority (self + N2 = 2/3) ──
        let leader_output = cluster.deliver_vote_response(NodeId(2), NodeId(1), &resp_n2);
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Leader);
        assert!(leader_output.events.iter().any(|e|
            matches!(e, ElectionEvent::BecameLeader { term } if *term == Term(1))
        ));

        // Leader clears its election deadline
        assert!(cluster.node(NodeId(1)).election_deadline().is_none());

        // Verify persisted state on N2
        let n2_saved = cluster.stores.get(&NodeId(2)).unwrap().last_saved().unwrap();
        assert_eq!(n2_saved.voted_for, Some(NodeId(1)));
        assert_eq!(n2_saved.current_term, Term(1));

        // N3's response arrives late — N1 already leader, no effect
        let late_output = cluster.deliver_vote_response(NodeId(3), NodeId(1), &resp_n3);
        assert!(late_output.is_empty());
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Leader);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Scenario: Split vote — neither candidate gets majority, retry
    //
    // Given N1 and N2 both become candidates for the same term,
    // When neither gets a majority (N3's messages are "lost"),
    // Then both remain Candidate and a new election starts with
    // incremented term on timeout.
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_split_vote_no_majority_retry() {
        let mut cluster = ClusterHarness::new();
        cluster.init_deadlines();

        // Advance clock past election timeout
        cluster.clock.advance(Duration::from_millis(250));

        // Both N1 and N2 time out simultaneously
        let n1_output = cluster.election_timeout(NodeId(1));
        let n2_output = cluster.election_timeout(NodeId(2));

        assert_eq!(cluster.node(NodeId(1)).role(), Role::Candidate);
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Candidate);
        assert_eq!(cluster.node(NodeId(1)).current_term(), Term(1));
        assert_eq!(cluster.node(NodeId(2)).current_term(), Term(1));

        let n1_req = extract_vote_request_from_sends(&n1_output);
        let n2_req = extract_vote_request_from_sends(&n2_output);

        // N1 and N2 exchange vote requests — both reject (already voted for self)
        let n2_to_n1 = cluster.deliver_vote_request(NodeId(1), NodeId(2), &n1_req);
        let (_, resp_n2_to_n1) = extract_vote_response_from_output(&n2_to_n1);
        assert!(!resp_n2_to_n1.vote_granted, "N2 already voted for self, must reject N1");

        let n1_to_n2 = cluster.deliver_vote_request(NodeId(2), NodeId(1), &n2_req);
        let (_, resp_n1_to_n2) = extract_vote_response_from_output(&n1_to_n2);
        assert!(!resp_n1_to_n2.vote_granted, "N1 already voted for self, must reject N2");

        // N3's messages are "lost" (not delivered) — true split vote scenario
        // Neither candidate has majority: N1 has {N1}, N2 has {N2}

        // Deliver rejections back
        let _ = cluster.deliver_vote_response(NodeId(2), NodeId(1), &resp_n2_to_n1);
        let _ = cluster.deliver_vote_response(NodeId(1), NodeId(2), &resp_n1_to_n2);

        // Both remain Candidate with only 1 vote each
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Candidate);
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Candidate);
        assert_eq!(cluster.node(NodeId(1)).votes_received().len(), 1);
        assert_eq!(cluster.node(NodeId(2)).votes_received().len(), 1);

        // Advance clock past the candidate's new election deadline
        cluster.clock.advance(Duration::from_millis(250));

        // N1 ticks — election timeout fires again → new election at term 2
        let n1_retry = cluster.tick_node(NodeId(1));
        assert_eq!(cluster.node(NodeId(1)).current_term(), Term(2));
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Candidate);
        assert!(n1_retry.events.iter().any(|e|
            matches!(e, ElectionEvent::ElectionStarted { term } if *term == Term(2))
        ));

        // Verify a new randomized deadline was set for this retry
        assert!(cluster.node(NodeId(1)).election_deadline().is_some());

        // N2 also retries → term 2
        let n2_retry = cluster.tick_node(NodeId(2));
        assert_eq!(cluster.node(NodeId(2)).current_term(), Term(2));
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Candidate);
        assert!(!n2_retry.persist_first.is_empty(), "retry must persist state");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Scenario: Split vote — N3 breaks the tie
    //
    // Both N1 and N2 become candidates. N3 votes for N1 (first request
    // received). N1 wins; N2 stays Candidate and retries.
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_split_vote_tie_broken_by_third_node() {
        let mut cluster = ClusterHarness::new();
        cluster.init_deadlines();
        cluster.clock.advance(Duration::from_millis(250));

        // Both N1 and N2 start elections for term 1
        let n1_output = cluster.election_timeout(NodeId(1));
        let n2_output = cluster.election_timeout(NodeId(2));
        let n1_req = extract_vote_request_from_sends(&n1_output);
        let n2_req = extract_vote_request_from_sends(&n2_output);

        // N3 receives N1's request first → grants
        let n3_grant = cluster.deliver_vote_request(NodeId(1), NodeId(3), &n1_req);
        let (_, resp_n3) = extract_vote_response_from_output(&n3_grant);
        assert!(resp_n3.vote_granted);

        // N3 receives N2's request → already voted for N1, rejects
        let n3_reject = cluster.deliver_vote_request(NodeId(2), NodeId(3), &n2_req);
        let (_, resp_n3_to_n2) = extract_vote_response_from_output(&n3_reject);
        assert!(!resp_n3_to_n2.vote_granted);

        // N2 rejects N1 (voted for self)
        let n2_to_n1 = cluster.deliver_vote_request(NodeId(1), NodeId(2), &n1_req);
        let (_, resp_n2_to_n1) = extract_vote_response_from_output(&n2_to_n1);
        assert!(!resp_n2_to_n1.vote_granted);

        // N1 rejects N2 (voted for self)
        let _ = cluster.deliver_vote_request(NodeId(2), NodeId(1), &n2_req);

        // Deliver N3's grant to N1 → majority (self + N3 = 2/3)
        let leader = cluster.deliver_vote_response(NodeId(3), NodeId(1), &resp_n3);
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Leader);
        assert!(leader.events.iter().any(|e| matches!(e, ElectionEvent::BecameLeader { .. })));

        // N2 gets N3's rejection, stays Candidate
        let _ = cluster.deliver_vote_response(NodeId(3), NodeId(2), &resp_n3_to_n2);
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Candidate);

        // N2's election deadline fires → new term
        cluster.clock.advance(Duration::from_millis(250));
        let n2_retry = cluster.tick_node(NodeId(2));
        assert_eq!(cluster.node(NodeId(2)).current_term(), Term(2));
        assert!(!n2_retry.then_send.is_empty(), "N2 should broadcast vote requests");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Scenario: Stale term rejection
    //
    // Given N1 is at term 5,
    // When N2 sends a VoteRequest for term 3,
    // Then N1 rejects the vote.
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stale_term_rejection() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(5), None);

        let stale_request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_offset: 10,
            last_log_term: Term(3),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &stale_request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);

        assert!(!response.vote_granted);
        assert_eq!(response.term, Term(5));
        assert_eq!(n1.current_term(), Term(5));
        // Stale request does not affect voted_for
        assert_eq!(n1.voted_for(), None);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Scenario: Log up-to-date check
    //
    // Given N1 has log ending at (term=3, offset=10) and N2 at (term=3, offset=8),
    // When N2 requests a vote,
    // Then N1 rejects because N2's log is less up-to-date.
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_log_up_to_date_rejection() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(4), None);
        n1.set_log_state(Term(3), 10);

        let request = VoteRequest {
            term: Term(4),
            candidate_id: NodeId(2),
            last_log_offset: 8,
            last_log_term: Term(3),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);

        assert!(!response.vote_granted);
        assert_eq!(response.term, Term(4));
        // voted_for should remain None — log not up-to-date
        assert_eq!(n1.voted_for(), None);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Higher term causes step-down via VoteResponse
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_higher_term_step_down_via_vote_response() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        let _ = n1.on_election_timeout(&clock);
        assert_eq!(n1.role(), Role::Candidate);

        let response = VoteResponse {
            term: Term(5),
            vote_granted: false,
            is_pre_vote: false,
        };
        let output = n1.handle_vote_response(NodeId(2), &response, &clock);

        assert_eq!(n1.role(), Role::Follower);
        assert_eq!(n1.current_term(), Term(5));
        assert_eq!(n1.voted_for(), None);
        assert!(output.events.iter().any(|e|
            matches!(e, ElectionEvent::SteppedDown { new_term } if *new_term == Term(5))
        ));
        // PersistQuorumState must be emitted for the step-down
        let qs = assert_has_persist(&output);
        assert_eq!(qs.current_term, Term(5));
        assert_eq!(qs.voted_for, None);
        // Election deadline was reset on step-down
        assert!(n1.election_deadline().is_some());
    }

    // ═══════════════════════════════════════════════════════════════════
    // VoteRequest with higher term: step-down then grant
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_vote_request_higher_term_step_down_and_grant() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(2), None);
        n1.set_log_state(Term(1), 5);

        let request = VoteRequest {
            term: Term(4),
            candidate_id: NodeId(2),
            last_log_offset: 5,
            last_log_term: Term(1),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);

        assert!(response.vote_granted);
        assert_eq!(response.term, Term(4));
        assert_eq!(n1.current_term(), Term(4));
        assert_eq!(n1.voted_for(), Some(NodeId(2)));
        assert_eq!(n1.role(), Role::Follower);
        assert!(output.events.iter().any(|e|
            matches!(e, ElectionEvent::SteppedDown { new_term } if *new_term == Term(4))
        ));
        // persist_first has PersistQuorumState
        assert_has_persist(&output);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Already voted for same candidate → grant
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_already_voted_same_candidate_grants() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), Some(NodeId(2)));
        n1.set_log_state(Term(2), 5);

        let request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_offset: 5,
            last_log_term: Term(2),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(response.vote_granted);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Already voted for different candidate → reject
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_already_voted_different_candidate_rejects() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), Some(NodeId(3)));
        n1.set_log_state(Term(2), 5);

        let request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_offset: 5,
            last_log_term: Term(2),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(!response.vote_granted);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Single-node cluster → immediate leader
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_single_node_immediate_leader() {
        let clock = SimulatedClock::new();
        let mut n1 = ElectionManager::new(
            NodeId(1),
            test_cluster_id(),
            vec![VoterInfo { node_id: NodeId(1), endpoint: "127.0.0.1:9001".into() }],
            RaftConfig::default(),
        );

        let output = n1.on_election_timeout(&clock);

        assert_eq!(n1.role(), Role::Leader);
        assert_eq!(n1.current_term(), Term(1));
        assert!(output.events.iter().any(|e| matches!(e, ElectionEvent::BecameLeader { .. })));
        assert_has_persist(&output);
        // Leader has no election deadline
        assert!(n1.election_deadline().is_none());
        // No sends needed (no peers)
        assert!(output.then_send.is_empty());
    }

    // ═══════════════════════════════════════════════════════════════════
    // fsync-before-ack: persist_first vs then_send separation
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_fsync_before_ack_ordering() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), None);
        n1.set_log_state(Term(2), 5);

        let request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_offset: 5,
            last_log_term: Term(2),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);

        // persist_first has PersistQuorumState
        let qs = assert_has_persist(&output);
        assert_eq!(qs.current_term, Term(3));
        assert_eq!(qs.voted_for, Some(NodeId(2)));

        // then_send has SendRpc with VoteResponse
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(response.vote_granted);

        // persist_first must not contain SendRpc
        for action in &output.persist_first.actions {
            assert!(!matches!(action, IoAction::SendRpc(_, _)),
                "persist_first must not contain SendRpc");
        }

        // then_send must not contain PersistQuorumState
        for action in &output.then_send.actions {
            assert!(!matches!(action, IoAction::PersistQuorumState(_)),
                "then_send must not contain PersistQuorumState");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Log comparison: higher term wins even with lower offset
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_log_comparison_higher_term_wins() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(5), None);
        n1.set_log_state(Term(2), 100);

        let request = VoteRequest {
            term: Term(5),
            candidate_id: NodeId(2),
            last_log_offset: 5,
            last_log_term: Term(4),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(response.vote_granted);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Leader does NOT start an election on timeout
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_leader_does_not_start_election() {
        let clock = SimulatedClock::new();
        let mut n1 = ElectionManager::new(
            NodeId(1),
            test_cluster_id(),
            vec![VoterInfo { node_id: NodeId(1), endpoint: "127.0.0.1:9001".into() }],
            RaftConfig::default(),
        );
        let _ = n1.on_election_timeout(&clock);
        assert_eq!(n1.role(), Role::Leader);

        let output = n1.on_election_timeout(&clock);
        assert!(output.is_empty());
        assert_eq!(n1.role(), Role::Leader);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Unknown/observer node cannot receive votes (membership check)
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_unknown_candidate_rejected() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), None);

        let request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(99),
            last_log_offset: 0,
            last_log_term: Term(3),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(99), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(!response.vote_granted);
        assert_eq!(n1.voted_for(), None);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Pre-vote requests are rejected (Stage 4.3 handles pre-vote)
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_pre_vote_rejected() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), None);

        let request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_offset: 0,
            last_log_term: Term(3),
            is_pre_vote: true,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(!response.vote_granted);
        assert!(response.is_pre_vote);
        assert_eq!(n1.voted_for(), None);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Simulated clock integration: deadline-driven election
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_simulated_clock_drives_election_via_tick() {
        let clock = SimulatedClock::new();
        clock.set_election_timeout(Duration::from_millis(200));

        let mut n1 = make_manager(1);
        n1.reset_election_deadline(&clock);

        // Before deadline — tick is a no-op
        let output = n1.tick(&clock);
        assert!(output.is_empty());
        assert_eq!(n1.role(), Role::Follower);

        // Advance past deadline
        clock.advance(Duration::from_millis(250));
        let output = n1.tick(&clock);
        assert_eq!(n1.role(), Role::Candidate);
        assert_eq!(n1.current_term(), Term(1));

        // Verify targeted vote requests
        let targets = extract_vote_request_targets(&output);
        assert_eq!(targets.len(), 2);
        assert_has_persist(&output);

        // A new deadline was set for the candidate retry
        assert!(n1.election_deadline().is_some());
    }

    // ═══════════════════════════════════════════════════════════════════
    // Election deadline lifecycle
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_election_deadline_lifecycle() {
        let clock = SimulatedClock::new();
        clock.set_election_timeout(Duration::from_millis(150));

        let mut n1 = make_manager(1);

        // Initially no deadline
        assert!(n1.election_deadline().is_none());

        // Set deadline
        n1.reset_election_deadline(&clock);
        let deadline1 = n1.election_deadline().unwrap();
        assert!(deadline1 > clock.now());

        // Granting a vote resets deadline
        let request = VoteRequest {
            term: Term(1),
            candidate_id: NodeId(2),
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: false,
        };
        clock.advance(Duration::from_millis(50));
        let _ = n1.handle_vote_request(NodeId(2), &request, &clock);
        let deadline2 = n1.election_deadline().unwrap();
        assert!(deadline2 > deadline1, "deadline should have been reset after granting vote");

        // Stale request does NOT reset deadline
        let stale_req = VoteRequest {
            term: Term(0),
            candidate_id: NodeId(3),
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: false,
        };
        let _ = n1.handle_vote_request(NodeId(3), &stale_req, &clock);
        let deadline3 = n1.election_deadline().unwrap();
        assert_eq!(deadline3, deadline2, "stale request should not reset deadline");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Envelope source != candidate_id → rejection
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_envelope_source_mismatch_rejected() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), None);

        let request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(3), // claims to be N3
            last_log_offset: 0,
            last_log_term: Term(3),
            is_pre_vote: false,
        };

        // Delivered by N2 but candidate_id says N3 → mismatch
        let output = n1.handle_vote_request(NodeId(2), &request, &clock);
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(!response.vote_granted);
        assert_eq!(n1.voted_for(), None);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Vote response from non-voter is ignored
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_vote_response_from_non_voter_ignored() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        let _ = n1.on_election_timeout(&clock);
        assert_eq!(n1.role(), Role::Candidate);
        assert_eq!(n1.votes_received().len(), 1); // self only

        let response = VoteResponse {
            term: Term(1),
            vote_granted: true,
            is_pre_vote: false,
        };

        // NodeId(99) is not a voter
        let output = n1.handle_vote_response(NodeId(99), &response, &clock);
        assert!(output.is_empty());
        assert_eq!(n1.votes_received().len(), 1, "non-voter grant must not count");
        assert_eq!(n1.role(), Role::Candidate);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Duplicate votes do not double-count
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_duplicate_votes_not_double_counted() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        let _ = n1.on_election_timeout(&clock);

        let grant = VoteResponse {
            term: Term(1),
            vote_granted: true,
            is_pre_vote: false,
        };

        // N2 grants twice
        let _ = n1.handle_vote_response(NodeId(2), &grant, &clock);
        assert_eq!(n1.role(), Role::Leader); // {self, N2} = 2/3
        assert_eq!(n1.votes_received().len(), 2);

        // Second grant from N2 — no effect (already leader)
        let output = n1.handle_vote_response(NodeId(2), &grant, &clock);
        assert!(output.is_empty());
    }

    // ═══════════════════════════════════════════════════════════════════
    // TERM-UPDATE ORDERING: higher-term pre-vote causes step-down
    //
    // A pre-vote request with a higher term must step down the node
    // (updating current_term and clearing voted_for) even though the
    // pre-vote itself is rejected.
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_higher_term_pre_vote_causes_step_down() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), Some(NodeId(1)));

        let pre_vote_req = VoteRequest {
            term: Term(7),
            candidate_id: NodeId(2),
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: true,
        };

        let output = n1.handle_vote_request(NodeId(2), &pre_vote_req, &clock);

        // Step-down occurred
        assert_eq!(n1.current_term(), Term(7));
        assert_eq!(n1.role(), Role::Follower);
        assert_eq!(n1.voted_for(), None);
        assert!(n1.election_deadline().is_some());

        // SteppedDown event emitted
        assert!(output.events.iter().any(|e|
            matches!(e, ElectionEvent::SteppedDown { new_term } if *new_term == Term(7))
        ));

        // Pre-vote is still rejected
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(!response.vote_granted);
        assert!(response.is_pre_vote);
        assert_eq!(response.term, Term(7));

        // Persist was emitted for the step-down
        let qs = assert_has_persist(&output);
        assert_eq!(qs.current_term, Term(7));
        assert_eq!(qs.voted_for, None);
    }

    // ═══════════════════════════════════════════════════════════════════
    // TERM-UPDATE ORDERING: higher-term from non-voter causes step-down
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_higher_term_non_voter_causes_step_down() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        let _ = n1.on_election_timeout(&clock);
        assert_eq!(n1.role(), Role::Candidate);
        assert_eq!(n1.current_term(), Term(1));

        let request = VoteRequest {
            term: Term(5),
            candidate_id: NodeId(99), // not in voter set
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(99), &request, &clock);

        // Step-down occurred before non-voter rejection
        assert_eq!(n1.current_term(), Term(5));
        assert_eq!(n1.role(), Role::Follower);
        assert_eq!(n1.voted_for(), None);
        assert!(n1.votes_received().is_empty());

        // SteppedDown event
        assert!(output.events.iter().any(|e|
            matches!(e, ElectionEvent::SteppedDown { new_term } if *new_term == Term(5))
        ));

        // Vote is rejected (non-voter)
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(!response.vote_granted);
        assert_eq!(response.term, Term(5));

        // Persist before send
        let qs = assert_has_persist(&output);
        assert_eq!(qs.current_term, Term(5));
    }

    // ═══════════════════════════════════════════════════════════════════
    // TERM-UPDATE ORDERING: higher-term from source-mismatch causes step-down
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_higher_term_source_mismatch_causes_step_down() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(2), Some(NodeId(1)));

        let request = VoteRequest {
            term: Term(6),
            candidate_id: NodeId(3), // claims N3
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: false,
        };

        // Delivered by N2 but candidate_id says N3 → source mismatch
        let output = n1.handle_vote_request(NodeId(2), &request, &clock);

        // Step-down occurred
        assert_eq!(n1.current_term(), Term(6));
        assert_eq!(n1.role(), Role::Follower);
        assert_eq!(n1.voted_for(), None);

        // SteppedDown event
        assert!(output.events.iter().any(|e|
            matches!(e, ElectionEvent::SteppedDown { new_term } if *new_term == Term(6))
        ));

        // Vote is rejected (source mismatch)
        let (_, response) = extract_vote_response_from_output(&output);
        assert!(!response.vote_granted);
        assert_eq!(response.term, Term(6));

        // Persist before send
        let qs = assert_has_persist(&output);
        assert_eq!(qs.current_term, Term(6));
    }

    // ═══════════════════════════════════════════════════════════════════
    // PRE-VOTE RESPONSE: pre-vote responses do NOT count as real votes
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_pre_vote_response_not_counted_as_real_vote() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        let _ = n1.on_election_timeout(&clock);
        assert_eq!(n1.role(), Role::Candidate);
        assert_eq!(n1.votes_received().len(), 1); // self

        let pre_vote_grant = VoteResponse {
            term: Term(1),
            vote_granted: true,
            is_pre_vote: true,
        };

        // N2 sends a pre-vote grant — must NOT be counted
        let output = n1.handle_vote_response(NodeId(2), &pre_vote_grant, &clock);
        assert!(output.is_empty());
        assert_eq!(n1.votes_received().len(), 1, "pre-vote must not count");
        assert_eq!(n1.role(), Role::Candidate, "must not become leader from pre-vote");

        // N3 also sends pre-vote — still not counted
        let output2 = n1.handle_vote_response(NodeId(3), &pre_vote_grant, &clock);
        assert!(output2.is_empty());
        assert_eq!(n1.role(), Role::Candidate);
    }

    // ═══════════════════════════════════════════════════════════════════
    // PRE-VOTE RESPONSE: higher-term pre-vote response still causes step-down
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_higher_term_pre_vote_response_causes_step_down() {
        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        let _ = n1.on_election_timeout(&clock);
        assert_eq!(n1.role(), Role::Candidate);

        let response = VoteResponse {
            term: Term(10),
            vote_granted: false,
            is_pre_vote: true,
        };

        let output = n1.handle_vote_response(NodeId(2), &response, &clock);

        // Higher term causes step-down even on pre-vote response
        assert_eq!(n1.current_term(), Term(10));
        assert_eq!(n1.role(), Role::Follower);
        assert_eq!(n1.voted_for(), None);

        assert!(output.events.iter().any(|e|
            matches!(e, ElectionEvent::SteppedDown { new_term } if *new_term == Term(10))
        ));
        let qs = assert_has_persist(&output);
        assert_eq!(qs.current_term, Term(10));
    }

    // ═══════════════════════════════════════════════════════════════════
    // Randomized election timeout produces varied values
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_randomized_election_timeout_varies() {
        let clock = SimulatedClock::new();
        clock.set_timeout_range(150, 300);

        let t1 = clock.random_election_timeout();
        let t2 = clock.random_election_timeout();
        let t3 = clock.random_election_timeout();

        // All within configured range
        for t in [t1, t2, t3] {
            assert!(t >= Duration::from_millis(150), "timeout {} too low", t.as_millis());
            assert!(t <= Duration::from_millis(300), "timeout {} too high", t.as_millis());
        }

        // At least two distinct values (deterministic but varied)
        assert!(t1 != t2 || t2 != t3,
            "expected varied timeouts, got: {:?}, {:?}, {:?}", t1, t2, t3);
    }

    // ═══════════════════════════════════════════════════════════════════
    // DEEP TEST: Full 3-node election with intermediate state checks
    //
    // Verifies every intermediate state, persistence, envelope, and event.
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_deep_3_node_election_with_all_assertions() {
        let mut cluster = ClusterHarness::new();
        cluster.init_deadlines();

        // All start as followers
        for id in 1..=3u64 {
            assert_eq!(cluster.node(NodeId(id)).role(), Role::Follower);
            assert_eq!(cluster.node(NodeId(id)).current_term(), Term(0));
            assert_eq!(cluster.node(NodeId(id)).voted_for(), None);
            assert!(cluster.node(NodeId(id)).election_deadline().is_some());
        }

        // Advance past timeout
        cluster.clock.advance(Duration::from_millis(350));

        // Only N1 times out (we trigger explicitly)
        let n1_output = cluster.election_timeout(NodeId(1));

        // N1 is now candidate for term 1
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Candidate);
        assert_eq!(cluster.node(NodeId(1)).current_term(), Term(1));
        assert_eq!(cluster.node(NodeId(1)).voted_for(), Some(NodeId(1)));
        assert_eq!(cluster.node(NodeId(1)).votes_received().len(), 1);
        assert!(cluster.node(NodeId(1)).votes_received().contains(&NodeId(1)));

        // Persistence: term=1, voted_for=N1
        let persisted_n1 = cluster.stores.get(&NodeId(1)).unwrap().last_saved().unwrap();
        assert_eq!(persisted_n1.current_term, Term(1));
        assert_eq!(persisted_n1.voted_for, Some(NodeId(1)));

        // Broadcasts to N2 and N3 only
        let targets = extract_vote_request_targets(&n1_output);
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&NodeId(2)));
        assert!(targets.contains(&NodeId(3)));
        assert!(!targets.contains(&NodeId(1)));

        // Verify envelope source on all RPCs
        assert_envelope_fields(&n1_output, NodeId(1), &test_cluster_id());

        let vote_req = extract_vote_request_from_sends(&n1_output);
        assert_eq!(vote_req.term, Term(1));
        assert_eq!(vote_req.candidate_id, NodeId(1));
        assert!(!vote_req.is_pre_vote);

        // N2 handles vote request: steps up to term 1, grants
        let n2_resp_output = cluster.deliver_vote_request(NodeId(1), NodeId(2), &vote_req);
        assert_eq!(cluster.node(NodeId(2)).current_term(), Term(1));
        assert_eq!(cluster.node(NodeId(2)).voted_for(), Some(NodeId(1)));
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Follower);

        // N2 persisted before responding
        let n2_saved = cluster.stores.get(&NodeId(2)).unwrap().last_saved().unwrap();
        assert_eq!(n2_saved.current_term, Term(1));
        assert_eq!(n2_saved.voted_for, Some(NodeId(1)));

        let (resp_target, resp_n2) = extract_vote_response_from_output(&n2_resp_output);
        assert_eq!(resp_target, NodeId(1));
        assert!(resp_n2.vote_granted);
        assert_eq!(resp_n2.term, Term(1));
        assert!(!resp_n2.is_pre_vote);

        // N3 also grants
        let n3_resp_output = cluster.deliver_vote_request(NodeId(1), NodeId(3), &vote_req);
        let (_, resp_n3) = extract_vote_response_from_output(&n3_resp_output);
        assert!(resp_n3.vote_granted);

        // N1 receives N2's grant → majority (self + N2 = 2/3)
        let leader_output = cluster.deliver_vote_response(NodeId(2), NodeId(1), &resp_n2);
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Leader);
        assert_eq!(cluster.node(NodeId(1)).current_term(), Term(1));
        assert!(cluster.node(NodeId(1)).election_deadline().is_none());
        assert_eq!(cluster.node(NodeId(1)).votes_received().len(), 2);

        assert!(leader_output.events.iter().any(|e|
            matches!(e, ElectionEvent::BecameLeader { term } if *term == Term(1))
        ));

        // N3's late response has no effect
        let late = cluster.deliver_vote_response(NodeId(3), NodeId(1), &resp_n3);
        assert!(late.is_empty());
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Leader);

        // Other nodes remain followers
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Follower);
        assert_eq!(cluster.node(NodeId(3)).role(), Role::Follower);
    }

    // ═══════════════════════════════════════════════════════════════════
    // DEEP TEST: Split vote with full message exchange
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_deep_split_vote_full_exchange() {
        let mut cluster = ClusterHarness::new();
        cluster.init_deadlines();
        cluster.clock.advance(Duration::from_millis(350));

        // N1 and N2 both start elections at term 1
        let n1_out = cluster.election_timeout(NodeId(1));
        let n2_out = cluster.election_timeout(NodeId(2));

        // Both are candidates with 1 self-vote
        assert_eq!(cluster.node(NodeId(1)).current_term(), Term(1));
        assert_eq!(cluster.node(NodeId(2)).current_term(), Term(1));
        assert_eq!(cluster.node(NodeId(1)).votes_received().len(), 1);
        assert_eq!(cluster.node(NodeId(2)).votes_received().len(), 1);

        let n1_req = extract_vote_request_from_sends(&n1_out);
        let n2_req = extract_vote_request_from_sends(&n2_out);

        // N1's request reaches N2: N2 rejects (already voted for self)
        let n2_handle_n1 = cluster.deliver_vote_request(NodeId(1), NodeId(2), &n1_req);
        let (_, resp_n2_to_n1) = extract_vote_response_from_output(&n2_handle_n1);
        assert!(!resp_n2_to_n1.vote_granted);
        assert_eq!(resp_n2_to_n1.term, Term(1));

        // N2's request reaches N1: N1 rejects (already voted for self)
        let n1_handle_n2 = cluster.deliver_vote_request(NodeId(2), NodeId(1), &n2_req);
        let (_, resp_n1_to_n2) = extract_vote_response_from_output(&n1_handle_n2);
        assert!(!resp_n1_to_n2.vote_granted);

        // N3 receives N1's request first, grants
        let n3_for_n1 = cluster.deliver_vote_request(NodeId(1), NodeId(3), &n1_req);
        let (_, resp_n3_n1) = extract_vote_response_from_output(&n3_for_n1);
        assert!(resp_n3_n1.vote_granted);
        assert_eq!(cluster.node(NodeId(3)).voted_for(), Some(NodeId(1)));

        // N3 receives N2's request, rejects (already voted for N1)
        let n3_for_n2 = cluster.deliver_vote_request(NodeId(2), NodeId(3), &n2_req);
        let (_, resp_n3_n2) = extract_vote_response_from_output(&n3_for_n2);
        assert!(!resp_n3_n2.vote_granted);

        // Deliver rejections to N1 and N2
        let _ = cluster.deliver_vote_response(NodeId(2), NodeId(1), &resp_n2_to_n1);
        let _ = cluster.deliver_vote_response(NodeId(1), NodeId(2), &resp_n1_to_n2);

        // N1 receives N3's grant → majority!
        let n1_leader = cluster.deliver_vote_response(NodeId(3), NodeId(1), &resp_n3_n1);
        assert_eq!(cluster.node(NodeId(1)).role(), Role::Leader);
        assert!(n1_leader.events.iter().any(|e| matches!(e, ElectionEvent::BecameLeader { .. })));

        // N2 receives N3's reject
        let _ = cluster.deliver_vote_response(NodeId(3), NodeId(2), &resp_n3_n2);
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Candidate);
        assert_eq!(cluster.node(NodeId(2)).votes_received().len(), 1);

        // N2 times out → new term
        cluster.clock.advance(Duration::from_millis(350));
        let n2_retry = cluster.tick_node(NodeId(2));
        assert_eq!(cluster.node(NodeId(2)).current_term(), Term(2));
        assert_eq!(cluster.node(NodeId(2)).role(), Role::Candidate);
        assert!(n2_retry.events.iter().any(|e|
            matches!(e, ElectionEvent::ElectionStarted { term } if *term == Term(2))
        ));
        assert!(!n2_retry.persist_first.is_empty());
        assert!(!n2_retry.then_send.is_empty());
    }

    // ═══════════════════════════════════════════════════════════════════
    // IoStage integration: persist-then-send contract
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_io_stage_enforces_persist_before_send() {
        use crate::io_stage::IoStage;

        let clock = SimulatedClock::new();
        let mut n1 = make_manager(1);
        n1.restore_state(Term(3), None);
        n1.set_log_state(Term(2), 5);

        let request = VoteRequest {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_offset: 5,
            last_log_term: Term(2),
            is_pre_vote: false,
        };

        let output = n1.handle_vote_request(NodeId(2), &request, &clock);

        // persist_first contains PersistQuorumState only
        assert!(!output.persist_first.is_empty());
        for action in &output.persist_first.actions {
            assert!(matches!(action, IoAction::PersistQuorumState(_)),
                "persist_first should only have PersistQuorumState");
        }

        // then_send contains SendRpc only
        assert!(!output.then_send.is_empty());
        for action in &output.then_send.actions {
            assert!(matches!(action, IoAction::SendRpc(_, _)),
                "then_send should only have SendRpc");
        }

        // IoStage converts to ordered actions correctly
        let ordered = IoStage::ordered_actions(output);
        // Persistence actions come first, then sends
        let persist_count = ordered.iter()
            .take_while(|a| matches!(a, IoAction::PersistQuorumState(_)))
            .count();
        assert!(persist_count > 0, "should start with persist actions");

        let send_count = ordered.iter()
            .skip(persist_count)
            .filter(|a| matches!(a, IoAction::SendRpc(_, _)))
            .count();
        assert!(send_count > 0, "should have send actions after persist");
    }

    // ═══════════════════════════════════════════════════════════════════
    // NodeState integration: dispatch VoteRequest through NodeEvent
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_node_state_dispatches_vote_request() {
        use crate::node_state::{NodeEvent, NodeState};

        let clock = SimulatedClock::new();
        let voter_set = three_node_voter_set();
        let mut state = NodeState::new(
            NodeId(1),
            test_cluster_id(),
            voter_set,
            RaftConfig::default(),
        );
        state.election_mut().restore_state(Term(3), None);
        state.election_mut().set_log_state(Term(2), 5);

        let envelope = RpcEnvelope {
            cluster_id: test_cluster_id(),
            leader_epoch: 0,
            source: NodeId(2),
            payload: RpcPayload::VoteRequest(VoteRequest {
                term: Term(3),
                candidate_id: NodeId(2),
                last_log_offset: 5,
                last_log_term: Term(2),
                is_pre_vote: false,
            }),
        };

        let event = NodeEvent::RpcReceived {
            from: NodeId(2),
            envelope,
        };

        let output = state.handle_event(event, &clock);

        // Should have granted the vote
        assert_eq!(state.election().current_term(), Term(3));
        assert_eq!(state.election().voted_for(), Some(NodeId(2)));
        assert!(!output.persist_first.is_empty());
        assert!(!output.then_send.is_empty());
    }

    // ═══════════════════════════════════════════════════════════════════
    // NodeState integration: dispatch Tick → election timeout
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_node_state_dispatches_tick() {
        use crate::node_state::{NodeEvent, NodeState};

        let clock = SimulatedClock::new();
        clock.set_election_timeout(Duration::from_millis(200));

        let voter_set = three_node_voter_set();
        let mut state = NodeState::new(
            NodeId(1),
            test_cluster_id(),
            voter_set,
            RaftConfig::default(),
        );
        state.election_mut().reset_election_deadline(&clock);

        // Tick before deadline: no-op
        let output = state.handle_event(NodeEvent::Tick, &clock);
        assert!(output.is_empty());
        assert_eq!(state.election().role(), Role::Follower);

        // Advance past deadline
        clock.advance(Duration::from_millis(250));

        let output = state.handle_event(NodeEvent::Tick, &clock);
        assert_eq!(state.election().role(), Role::Candidate);
        assert_eq!(state.election().current_term(), Term(1));
        assert!(!output.persist_first.is_empty());
        assert!(!output.then_send.is_empty());
    }

    // ═══════════════════════════════════════════════════════════════════
    // NodeState integration: dispatch VoteResponse → leader transition
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_node_state_vote_response_to_leader() {
        use crate::node_state::{NodeEvent, NodeState};

        let clock = SimulatedClock::new();
        let voter_set = three_node_voter_set();
        let mut state = NodeState::new(
            NodeId(1),
            test_cluster_id(),
            voter_set,
            RaftConfig::default(),
        );

        // Start election
        let _ = state.election_mut().on_election_timeout(&clock);
        assert_eq!(state.election().role(), Role::Candidate);

        // Deliver vote from N2
        let envelope = RpcEnvelope {
            cluster_id: test_cluster_id(),
            leader_epoch: 0,
            source: NodeId(2),
            payload: RpcPayload::VoteResponse(VoteResponse {
                term: Term(1),
                vote_granted: true,
                is_pre_vote: false,
            }),
        };

        let output = state.handle_event(
            NodeEvent::RpcReceived {
                from: NodeId(2),
                envelope,
            },
            &clock,
        );

        assert_eq!(state.election().role(), Role::Leader);
        assert!(output.events.iter().any(|e| matches!(e, ElectionEvent::BecameLeader { .. })));
    }
}
