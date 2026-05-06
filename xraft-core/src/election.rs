use crate::consensus_state::Role;
use crate::node_state::{ElectionPhase, NodeState};
use crate::quorum_state::QuorumState;
use crate::rpc::{FetchResponse, VoteRequest, VoteResponse};
use crate::types::{NodeId, Term};

/// Inbound events that the event loop delivers to the election subsystem.
///
/// The event loop's main `select!` / poll loop converts raw I/O events
/// (timer firings, deserialized RPC messages) into `ElectionEvent` values
/// and passes them to [`ElectionDriver::handle`], which returns the
/// [`ElectionAction`](s) the event loop must execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionEvent {
    /// The election timer expired. This is the single entry point for the
    /// election lifecycle: timeout → pre-vote → (majority?) → real election.
    ElectionTimeout,

    /// A `VoteRequest` (pre-vote or real) arrived from a peer.
    VoteRequestReceived {
        request: VoteRequest,
        /// Current wall-clock time in milliseconds.
        now_ms: u64,
        /// Election timeout threshold for the leader-lease rejection rule.
        election_timeout_ms: u64,
    },

    /// A `VoteResponse` (pre-vote or real) arrived from a peer.
    VoteResponseReceived {
        from: NodeId,
        response: VoteResponse,
    },

    /// A `FetchResponse` arrived from the leader. This is the primary
    /// mechanism through which followers learn the leader is alive. The
    /// election subsystem uses this to maintain the leader-lease timer
    /// that gates pre-vote rejection.
    FetchResponseReceived {
        response: FetchResponse,
        /// Current wall-clock time in milliseconds.
        now_ms: u64,
    },
}

/// Actions produced by the election driver for the event loop to execute.
/// A single event may produce multiple actions (e.g., StartRealElection
/// followed by PersistAndSendVoteRequests when the pre-vote majority is
/// immediate). The driver collects them in order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ElectionActionBatch {
    pub actions: Vec<ElectionAction>,
}

impl ElectionActionBatch {
    fn single(action: ElectionAction) -> Self {
        if action == ElectionAction::None {
            Self { actions: vec![] }
        } else {
            Self { actions: vec![action] }
        }
    }

    fn push(&mut self, action: ElectionAction) {
        if action != ElectionAction::None {
            self.actions.push(action);
        }
    }

    /// Returns true if the batch contains no meaningful actions.
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

/// Event-loop integration driver for the election subsystem.
///
/// This is the **only** entry point the event loop should use for election
/// logic. It accepts [`ElectionEvent`]s and returns [`ElectionActionBatch`]es
/// that the event loop must execute in order (persist, send, transition).
///
/// The driver enforces the pre-vote gate: the only path to a real election
/// is through a successful pre-vote majority. There is no public method to
/// bypass this gate.
///
/// ## Event loop dispatch pattern
///
/// ```ignore
/// // In the event loop's main select!/poll:
/// let batch = ElectionDriver::handle(&mut node_state, event);
/// for action in batch.actions {
///     match action {
///         ElectionAction::SendPreVoteRequests(targets) => { /* send RPCs */ }
///         ElectionAction::PersistAndSendVoteRequests { .. } => { /* fsync then send */ }
///         ElectionAction::PersistAndRespondVote { .. } => { /* fsync then respond */ }
///         ElectionAction::RespondPreVote(resp) => { /* respond immediately */ }
///         ElectionAction::BecomeLeader => { /* transition to leader */ }
///         ElectionAction::PersistAndStepDown { .. } => { /* fsync new term */ }
///         ElectionAction::None | ElectionAction::StartRealElection => { /* internal */ }
///     }
/// }
/// ```
pub struct ElectionDriver;

impl ElectionDriver {
    /// Process an election event and return the batch of actions.
    ///
    /// This is the single entry point for all election-related events.
    /// The event loop should call this for every election-related event
    /// and execute the returned actions in order.
    pub fn handle(state: &mut NodeState, event: ElectionEvent) -> ElectionActionBatch {
        match event {
            ElectionEvent::ElectionTimeout => {
                let action = ElectionManager::start_pre_vote(state);
                Self::resolve_chain(state, action)
            }
            ElectionEvent::VoteRequestReceived {
                request,
                now_ms,
                election_timeout_ms,
            } => {
                let action = ElectionManager::handle_vote_request(
                    state,
                    &request,
                    now_ms,
                    election_timeout_ms,
                );
                ElectionActionBatch::single(action)
            }
            ElectionEvent::VoteResponseReceived { from, response } => {
                let action =
                    ElectionManager::handle_vote_response(state, from, &response);
                Self::resolve_chain(state, action)
            }
            ElectionEvent::FetchResponseReceived { response, now_ms } => {
                let action =
                    ElectionManager::handle_fetch_response(state, &response, now_ms);
                ElectionActionBatch::single(action)
            }
        }
    }

    /// If an action is `StartRealElection`, chain into the real election
    /// and collect both the signal and the resulting action.
    fn resolve_chain(state: &mut NodeState, action: ElectionAction) -> ElectionActionBatch {
        if action == ElectionAction::StartRealElection {
            let real = ElectionManager::start_real_election(state);
            let mut batch = ElectionActionBatch::default();
            batch.push(real);
            batch
        } else {
            ElectionActionBatch::single(action)
        }
    }
}

/// Actions produced by `ElectionManager` methods.
///
/// The event loop inspects the returned action and performs the corresponding
/// I/O (persistence, network sends, state-machine transitions). This keeps the
/// `ElectionManager` itself free of async or I/O dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionAction {
    /// No further action required.
    None,

    /// Broadcast pre-vote requests to the listed peers.
    /// No durable state has been mutated — the event loop must NOT persist
    /// term or voted_for before sending these.
    SendPreVoteRequests(Vec<(NodeId, VoteRequest)>),

    /// The pre-vote round succeeded. The event loop should call
    /// `start_real_election` next (after optionally re-checking the leader
    /// lease).
    StartRealElection,

    /// Persist `current_term` and `voted_for` **before** broadcasting the
    /// real vote requests. Violating this ordering breaks Raft's durability
    /// guarantee.
    PersistAndSendVoteRequests {
        term: Term,
        voted_for: NodeId,
        requests: Vec<(NodeId, VoteRequest)>,
    },

    /// Respond to a real vote request. The event loop must persist
    /// `current_term` and `voted_for` **before** sending the response.
    PersistAndRespondVote {
        term: Term,
        voted_for: Option<NodeId>,
        response: VoteResponse,
    },

    /// Respond to a pre-vote request. No persistence required.
    RespondPreVote(VoteResponse),

    /// The node won a real election — transition to Leader.
    BecomeLeader,

    /// A response carried a higher term. Step down to Follower and adopt it.
    /// The event loop **must** persist `new_term` and `voted_for = None`
    /// before processing any further messages.
    PersistAndStepDown { new_term: Term },
}

/// Stateless election logic. All mutable state lives in [`NodeState`]; the
/// manager reads/writes it through `&mut NodeState` references and returns
/// [`ElectionAction`]s that the event loop must execute.
pub struct ElectionManager;

impl ElectionManager {
    // ── Pre-Vote initiation ──────────────────────────────────────────

    /// Begin the pre-vote phase. The node does NOT increment its term or
    /// persist any state — it sends speculative `VoteRequest`s with
    /// `is_pre_vote = true` at `current_term + 1`.
    pub fn start_pre_vote(state: &mut NodeState) -> ElectionAction {
        // Guard: only voters can initiate elections. Observers or removed
        // nodes must not self-count or send pre-vote requests.
        if !state.is_voter(state.node_id) {
            return ElectionAction::None;
        }

        // Role stays as-is (typically Follower) — only election_phase changes.
        state.election_phase = ElectionPhase::PreVote;
        state.pre_votes_received.clear();
        state.pre_votes_received.insert(state.node_id); // vote for self

        let prospective_term = Term(state.current_term.0.saturating_add(1));
        state.pre_vote_term = Some(prospective_term);

        // Single-node cluster: self-vote is already a majority.
        if state.pre_votes_received.len() >= state.majority() {
            return ElectionAction::StartRealElection;
        }

        let request = VoteRequest {
            term: prospective_term,
            candidate_id: state.node_id,
            last_log_offset: state.log_end_offset,
            last_log_term: state.last_log_term,
            is_pre_vote: true,
        };

        let targets: Vec<(NodeId, VoteRequest)> = state
            .other_voters()
            .into_iter()
            .map(|id| (id, request.clone()))
            .collect();

        ElectionAction::SendPreVoteRequests(targets)
    }

    // ── Real election initiation ─────────────────────────────────────

    /// Begin the real election. Only proceeds if a pre-vote majority was
    /// received (enforced by checking `election_phase` and the pre-vote
    /// tally). Single-node clusters bypass via `StartRealElection` from
    /// `start_pre_vote`, which is the only valid path here.
    ///
    /// Increments term, votes for self, and returns an action that requires
    /// persistence before the requests are sent.
    pub fn start_real_election(state: &mut NodeState) -> ElectionAction {
        // Guard: only voters can start elections.
        if !state.is_voter(state.node_id) {
            return ElectionAction::None;
        }

        // Gate: only proceed if we completed pre-vote successfully.
        if state.election_phase != ElectionPhase::PreVote
            || state.pre_votes_received.len() < state.majority()
        {
            return ElectionAction::None;
        }

        state.current_term = Term(state.current_term.0.saturating_add(1));
        state.voted_for = Some(state.node_id);
        state.role = Role::Candidate;
        state.election_phase = ElectionPhase::Election;
        state.leader_id = None;
        state.pre_vote_term = None;
        state.votes_received.clear();
        state.votes_received.insert(state.node_id); // vote for self

        // Single-node cluster.
        if state.votes_received.len() >= state.majority() {
            return ElectionAction::BecomeLeader;
        }

        let request = VoteRequest {
            term: state.current_term,
            candidate_id: state.node_id,
            last_log_offset: state.log_end_offset,
            last_log_term: state.last_log_term,
            is_pre_vote: false,
        };

        let targets: Vec<(NodeId, VoteRequest)> = state
            .other_voters()
            .into_iter()
            .map(|id| (id, request.clone()))
            .collect();

        ElectionAction::PersistAndSendVoteRequests {
            term: state.current_term,
            voted_for: state.node_id,
            requests: targets,
        }
    }

    // ── Handling inbound VoteRequest ──────────────────────────────────

    /// Process a `VoteRequest` (pre-vote or real) from a peer.
    ///
    /// * `now_ms` — current wall-clock time in milliseconds (from `Clock`).
    /// * `election_timeout_ms` — the election timeout threshold used for the
    ///   leader-lease rejection rule (typically `election_timeout_min`).
    pub fn handle_vote_request(
        state: &mut NodeState,
        request: &VoteRequest,
        now_ms: u64,
        election_timeout_ms: u64,
    ) -> ElectionAction {
        if request.is_pre_vote {
            Self::handle_pre_vote_request(state, request, now_ms, election_timeout_ms)
        } else {
            Self::handle_real_vote_request(state, request)
        }
    }

    fn handle_pre_vote_request(
        state: &mut NodeState,
        request: &VoteRequest,
        now_ms: u64,
        election_timeout_ms: u64,
    ) -> ElectionAction {
        // Reject if the candidate is not in the voter set. Observers or
        // removed nodes must not be able to trigger elections.
        if !state.is_voter(request.candidate_id) {
            return ElectionAction::RespondPreVote(VoteResponse {
                term: state.current_term,
                vote_granted: false,
                is_pre_vote: true,
            });
        }

        // Reject if the prospective term is stale.
        if request.term < state.current_term {
            return ElectionAction::RespondPreVote(VoteResponse {
                term: state.current_term,
                vote_granted: false,
                is_pre_vote: true,
            });
        }

        // Leader lease check: reject if we recently heard from a valid leader
        // whose term matches our current term AND who is a member of the voter set.
        if let Some(last_contact) = state.last_leader_contact_ms {
            let within_timeout = now_ms.saturating_sub(last_contact) < election_timeout_ms;
            let leader_valid = if let Some(lid) = state.leader_id {
                // The leader must be in the current voter set (a legitimate member)
                // and the contact must be from the current term.
                let is_member = state.is_voter(lid);
                let is_current_term = state.last_leader_term == Some(state.current_term);
                is_member && is_current_term
            } else {
                false
            };
            if within_timeout && leader_valid {
                return ElectionAction::RespondPreVote(VoteResponse {
                    term: state.current_term,
                    vote_granted: false,
                    is_pre_vote: true,
                });
            }
        }

        // Log up-to-date check (§5.4.1 of the Raft paper).
        let grant = Self::is_log_up_to_date(
            state,
            request.last_log_term,
            request.last_log_offset,
        );

        // Pre-vote: do NOT update term, do NOT set voted_for.
        ElectionAction::RespondPreVote(VoteResponse {
            term: state.current_term,
            vote_granted: grant,
            is_pre_vote: true,
        })
    }

    fn handle_real_vote_request(
        state: &mut NodeState,
        request: &VoteRequest,
    ) -> ElectionAction {
        // Reject if the candidate is not in the voter set. Must check
        // before adopting a higher term — otherwise a non-voter can force
        // voters to step down by sending a high term.
        if !state.is_voter(request.candidate_id) {
            return ElectionAction::PersistAndRespondVote {
                term: state.current_term,
                voted_for: state.voted_for,
                response: VoteResponse {
                    term: state.current_term,
                    vote_granted: false,
                    is_pre_vote: false,
                },
            };
        }

        // Higher term → step down and adopt.
        if request.term > state.current_term {
            state.current_term = request.term;
            state.voted_for = None;
            state.role = Role::Follower;
            state.leader_id = None;
            state.election_phase = ElectionPhase::None;
            state.votes_received.clear();
            state.pre_votes_received.clear();
            state.pre_vote_term = None;
        }

        // Reject if the requester's term is stale.
        if request.term < state.current_term {
            return ElectionAction::PersistAndRespondVote {
                term: state.current_term,
                voted_for: state.voted_for,
                response: VoteResponse {
                    term: state.current_term,
                    vote_granted: false,
                    is_pre_vote: false,
                },
            };
        }

        // Grant conditions:
        // (a) haven't voted in this term, or already voted for this candidate
        let can_vote = state.voted_for.is_none()
            || state.voted_for == Some(request.candidate_id);

        // (b) candidate's log is at least as up-to-date
        let log_ok = Self::is_log_up_to_date(
            state,
            request.last_log_term,
            request.last_log_offset,
        );

        let grant = can_vote && log_ok;

        if grant {
            state.voted_for = Some(request.candidate_id);
        }

        ElectionAction::PersistAndRespondVote {
            term: state.current_term,
            voted_for: state.voted_for,
            response: VoteResponse {
                term: state.current_term,
                vote_granted: grant,
                is_pre_vote: false,
            },
        }
    }

    // ── Handling inbound VoteResponse ─────────────────────────────────

    /// Process a `VoteResponse` received from `from`.
    pub fn handle_vote_response(
        state: &mut NodeState,
        from: NodeId,
        response: &VoteResponse,
    ) -> ElectionAction {
        // Ignore if sender is not in the voter set. Must check before
        // higher-term adoption — otherwise a non-voter/observer can force
        // stepdown by sending a high term in a response.
        if !state.is_voter(from) {
            return ElectionAction::None;
        }

        // Higher term in response → step down regardless of pre-vote/real.
        if response.term > state.current_term {
            state.current_term = response.term;
            state.voted_for = None;
            state.role = Role::Follower;
            state.leader_id = None;
            state.election_phase = ElectionPhase::None;
            state.votes_received.clear();
            state.pre_votes_received.clear();
            state.pre_vote_term = None;
            return ElectionAction::PersistAndStepDown {
                new_term: response.term,
            };
        }

        if response.is_pre_vote {
            Self::handle_pre_vote_response(state, from, response)
        } else {
            Self::handle_real_vote_response(state, from, response)
        }
    }

    fn handle_pre_vote_response(
        state: &mut NodeState,
        from: NodeId,
        response: &VoteResponse,
    ) -> ElectionAction {
        // Ignore if we're not in the pre-vote phase.
        if state.election_phase != ElectionPhase::PreVote {
            return ElectionAction::None;
        }

        // Validate that we have an active pre-vote round. Responses must belong
        // to the current round — the higher-term step-down check above already
        // handles responses with term > current_term.
        if state.pre_vote_term.is_none() {
            return ElectionAction::None;
        }

        // Reject stale responses: if the responder's term is below our
        // current term, the response is from an outdated round and must
        // not be counted. A legitimate pre-vote responder should be at
        // least at our term.
        if response.term < state.current_term {
            return ElectionAction::None;
        }

        if !response.vote_granted {
            return ElectionAction::None;
        }

        state.pre_votes_received.insert(from);

        if state.pre_votes_received.len() >= state.majority() {
            ElectionAction::StartRealElection
        } else {
            ElectionAction::None
        }
    }

    fn handle_real_vote_response(
        state: &mut NodeState,
        from: NodeId,
        response: &VoteResponse,
    ) -> ElectionAction {
        // Ignore if we're not in a real election or not a candidate.
        if state.election_phase != ElectionPhase::Election
            || state.role != Role::Candidate
        {
            return ElectionAction::None;
        }

        // Ignore if term doesn't match our current term.
        if response.term != state.current_term {
            return ElectionAction::None;
        }

        if !response.vote_granted {
            return ElectionAction::None;
        }

        state.votes_received.insert(from);

        if state.votes_received.len() >= state.majority() {
            ElectionAction::BecomeLeader
        } else {
            ElectionAction::None
        }
    }

    // ── Event-loop integration (legacy convenience) ─────────────────
    //
    // Prefer `ElectionDriver::handle(state, event)` for new code.
    // These methods remain public for backward compatibility but delegate
    // to the same core logic that `ElectionDriver` uses.

    /// Called by the event loop when the election timeout fires.
    /// Delegates to `start_pre_vote` and chains into `start_real_election`
    /// when a single-node cluster gets an immediate majority.
    ///
    /// **Preferred:** Use `ElectionDriver::handle(state, ElectionEvent::ElectionTimeout)`.
    pub fn on_election_timeout(state: &mut NodeState) -> ElectionAction {
        let action = Self::start_pre_vote(state);
        if action == ElectionAction::StartRealElection {
            return Self::start_real_election(state);
        }
        action
    }

    /// Called by the event loop when it receives a `StartRealElection` action.
    ///
    /// **Preferred:** Use `ElectionDriver::handle` which chains automatically.
    pub fn proceed_to_real_election(state: &mut NodeState) -> ElectionAction {
        Self::start_real_election(state)
    }

    // ── Helpers ──────────────────────────────────────────────────────

    /// Returns `true` if the candidate's log is at least as up-to-date as
    /// the voter's log per §5.4.1 of the Raft paper:
    /// - higher last log term wins, or
    /// - same term ⇒ higher (or equal) last log offset wins.
    fn is_log_up_to_date(
        state: &NodeState,
        candidate_last_term: Term,
        candidate_last_offset: crate::types::Offset,
    ) -> bool {
        if candidate_last_term != state.last_log_term {
            return candidate_last_term > state.last_log_term;
        }
        candidate_last_offset >= state.log_end_offset
    }

    // ── Fetch response handling ─────────────────────────────────────

    /// Process a `FetchResponse` from the leader. Records leader contact
    /// timestamp if the response is from a valid current-term leader that
    /// is a member of the voter set.
    pub fn handle_fetch_response(
        state: &mut NodeState,
        response: &FetchResponse,
        now_ms: u64,
    ) -> ElectionAction {
        // Higher term → step down and adopt the new term.
        // Validate leader membership BEFORE recording it — a non-member
        // leader should not be recorded, even though we must still adopt
        // the higher term.
        if response.term > state.current_term {
            let leader_is_member = state.is_voter(response.leader_id);

            state.current_term = response.term;
            state.voted_for = None;
            state.role = Role::Follower;
            state.election_phase = ElectionPhase::None;
            state.votes_received.clear();
            state.pre_votes_received.clear();
            state.pre_vote_term = None;

            if leader_is_member {
                state.leader_id = Some(response.leader_id);
                state.record_leader_contact(now_ms, response.leader_id, response.term);
            } else {
                state.leader_id = None;
            }

            return ElectionAction::PersistAndStepDown {
                new_term: response.term,
            };
        }

        // Stale term — ignore.
        if response.term < state.current_term {
            return ElectionAction::None;
        }

        // Same term: validate that the leader is a voter set member.
        if !state.is_voter(response.leader_id) {
            return ElectionAction::None;
        }

        // Record valid leader contact at current term.
        state.record_leader_contact(now_ms, response.leader_id, response.term);
        ElectionAction::None
    }
}

// ── ElectionExecutor ────────────────────────────────────────────────

use crate::quorum_state::QuorumStateStore;

/// Executes [`ElectionActionBatch`]es against a [`QuorumStateStore`],
/// enforcing the critical ordering invariant: durable state is persisted
/// **before** any network I/O (vote responses, vote requests, leader
/// transitions).
///
/// This is the integration layer between the pure-logic [`ElectionManager`]
/// and the I/O world. The event loop should use `ElectionExecutor` instead
/// of manually matching on action variants.
pub struct ElectionExecutor;

/// Results of executing an action batch. The event loop inspects these
/// to perform the actual network sends.
#[derive(Debug, Default)]
pub struct ExecutionResult {
    /// Pre-vote requests to send (no persistence needed).
    pub pre_vote_requests: Vec<(NodeId, VoteRequest)>,
    /// Real vote requests to send (persistence already completed).
    pub vote_requests: Vec<(NodeId, VoteRequest)>,
    /// Pre-vote response to send (no persistence needed).
    pub pre_vote_response: Option<VoteResponse>,
    /// Real vote response to send (persistence already completed).
    pub vote_response: Option<VoteResponse>,
    /// Whether the node became leader.
    pub became_leader: bool,
    /// Whether persistence failed (event loop should handle as fatal).
    pub persist_failed: Option<String>,
}

/// Snapshot of `NodeState` fields that may be mutated by `ElectionManager`
/// during action processing. Used by the executor to roll back state if
/// persistence fails, ensuring NodeState and durable state stay consistent.
///
/// Capture this **before** calling `ElectionDriver::handle`, then pass it
/// to `ElectionExecutor::execute`.
#[derive(Debug, Clone)]
pub struct NodeStateSnapshot {
    current_term: Term,
    voted_for: Option<NodeId>,
    role: Role,
    leader_id: Option<NodeId>,
    election_phase: ElectionPhase,
    votes_received: std::collections::HashSet<NodeId>,
    pre_votes_received: std::collections::HashSet<NodeId>,
    pre_vote_term: Option<Term>,
    last_leader_contact_ms: Option<u64>,
    last_leader_term: Option<Term>,
}

impl NodeStateSnapshot {
    /// Capture a snapshot of the mutable election-relevant fields.
    pub fn capture(state: &NodeState) -> Self {
        Self {
            current_term: state.current_term,
            voted_for: state.voted_for,
            role: state.role,
            leader_id: state.leader_id,
            election_phase: state.election_phase,
            votes_received: state.votes_received.clone(),
            pre_votes_received: state.pre_votes_received.clone(),
            pre_vote_term: state.pre_vote_term,
            last_leader_contact_ms: state.last_leader_contact_ms,
            last_leader_term: state.last_leader_term,
        }
    }

    fn restore(self, state: &mut NodeState) {
        state.current_term = self.current_term;
        state.voted_for = self.voted_for;
        state.role = self.role;
        state.leader_id = self.leader_id;
        state.election_phase = self.election_phase;
        state.votes_received = self.votes_received;
        state.pre_votes_received = self.pre_votes_received;
        state.pre_vote_term = self.pre_vote_term;
        state.last_leader_contact_ms = self.last_leader_contact_ms;
        state.last_leader_term = self.last_leader_term;
    }
}

impl ElectionExecutor {
    /// Execute an [`ElectionActionBatch`] against the given store.
    ///
    /// Takes `&mut NodeState` so that on persistence failure, the executor
    /// can **roll back** all in-memory state changes that `ElectionManager`
    /// made during action computation. This guarantees that `NodeState` and
    /// durable `QuorumState` never diverge — if persistence fails, the node
    /// is left in its pre-event state and the event loop should treat this
    /// as a fatal error (restart / re-load from store).
    ///
    /// On success, persistence is completed before any sends are recorded
    /// in the result.
    pub fn execute(
        state: &mut NodeState,
        batch: &ElectionActionBatch,
        store: &mut dyn QuorumStateStore,
        snapshot: &NodeStateSnapshot,
    ) -> ExecutionResult {
        let mut result = ExecutionResult::default();

        for action in &batch.actions {
            match action {
                ElectionAction::SendPreVoteRequests(targets) => {
                    // No persistence required for pre-vote.
                    result.pre_vote_requests.extend(targets.iter().cloned());
                }
                ElectionAction::PersistAndSendVoteRequests {
                    term,
                    voted_for,
                    requests,
                } => {
                    let qs = QuorumState {
                        current_term: *term,
                        voted_for: Some(*voted_for),
                        leader_id: state.leader_id,
                        leader_epoch: 0,
                    };
                    if let Err(e) = store.persist(&qs) {
                        snapshot.clone().restore(state);
                        result.persist_failed = Some(e);
                        return result;
                    }
                    result.vote_requests.extend(requests.iter().cloned());
                }
                ElectionAction::PersistAndRespondVote {
                    term,
                    voted_for,
                    response,
                } => {
                    let qs = QuorumState {
                        current_term: *term,
                        voted_for: *voted_for,
                        leader_id: state.leader_id,
                        leader_epoch: 0,
                    };
                    if let Err(e) = store.persist(&qs) {
                        snapshot.clone().restore(state);
                        result.persist_failed = Some(e);
                        return result;
                    }
                    result.vote_response = Some(response.clone());
                }
                ElectionAction::RespondPreVote(resp) => {
                    // No persistence required.
                    result.pre_vote_response = Some(resp.clone());
                }
                ElectionAction::BecomeLeader => {
                    let qs = QuorumState {
                        current_term: state.current_term,
                        voted_for: state.voted_for,
                        leader_id: Some(state.node_id),
                        leader_epoch: 0,
                    };
                    if let Err(e) = store.persist(&qs) {
                        snapshot.clone().restore(state);
                        result.persist_failed = Some(e);
                        return result;
                    }
                    result.became_leader = true;
                }
                ElectionAction::PersistAndStepDown { new_term } => {
                    let qs = QuorumState {
                        current_term: *new_term,
                        voted_for: None,
                        leader_id: state.leader_id,
                        leader_epoch: 0,
                    };
                    if let Err(e) = store.persist(&qs) {
                        snapshot.clone().restore(state);
                        result.persist_failed = Some(e);
                        return result;
                    }
                }
                ElectionAction::StartRealElection | ElectionAction::None => {}
            }
        }

        result
    }

    /// Convenience wrapper: snapshots state, calls `ElectionDriver::handle`,
    /// then executes the batch through the store. This is the preferred
    /// single-call entry point for the event loop.
    pub fn handle_event(
        state: &mut NodeState,
        event: ElectionEvent,
        store: &mut dyn QuorumStateStore,
    ) -> (ElectionActionBatch, ExecutionResult) {
        let snapshot = NodeStateSnapshot::capture(state);
        let batch = ElectionDriver::handle(state, event);
        let result = Self::execute(state, &batch, store, &snapshot);
        (batch, result)
    }

    /// Load persisted quorum state and apply it to `NodeState`.
    /// Used on startup/recovery to restore durable election state.
    pub fn load_and_restore(
        state: &mut NodeState,
        store: &dyn QuorumStateStore,
    ) -> Result<(), String> {
        if let Some(qs) = store.load()? {
            state.current_term = qs.current_term;
            state.voted_for = qs.voted_for;
            state.leader_id = qs.leader_id;
        }
        Ok(())
    }
}
