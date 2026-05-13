use std::collections::HashSet;

use bytes::Bytes;

use crate::config::RaftConfig;
use crate::consensus_state::{ConsensusState, Role};
use crate::error::XraftError;
use crate::log_entry::{EntryType, LogEntry};
use crate::node_state::NodeState;
use crate::listener::Listener;
use crate::quorum_state::QuorumState;
use crate::rpc::{RpcEnvelope, RpcPayload, VoteRequest, VoteResponse};
use crate::traits::{
    Clock, LogStore, QuorumStateStore, SnapshotIO, StateMachine, TransportReceiver,
    TransportSender,
};
use crate::types::{ClusterId, NodeId, Term};
use crate::voter::{VoterInfo, VotersRecord};

/// Public handle for a Raft consensus node.
///
/// Generic over `S: StateMachine` and `L: Listener` for zero-cost dispatch.
/// I/O and runtime traits are injected as `Box<dyn ...>` trait objects.
pub struct RaftNode<S: StateMachine, L: Listener> {
    pub(crate) state: NodeState,
    pub(crate) config: RaftConfig,
    pub log_store: Box<dyn LogStore>,
    pub quorum_state_store: Box<dyn QuorumStateStore>,
    pub snapshot_io: Box<dyn SnapshotIO>,
    pub(crate) transport_sender: Box<dyn TransportSender>,
    pub(crate) transport_receiver: Box<dyn TransportReceiver>,
    pub(crate) clock: Box<dyn Clock>,
    pub(crate) state_machine: S,
    pub(crate) listener: L,
    pub(crate) bootstrapped: bool,
}

impl<S: StateMachine, L: Listener> RaftNode<S, L> {
    /// Creates a new `RaftNode`.
    ///
    /// Uses `config.node_id` as the node's identity. If existing data is
    /// detected (log, quorum-state, or snapshot), calls `recover()` to restore
    /// state from persisted data and transitions to Follower. If the data
    /// directory is empty, the node starts in `Unattached` role and waits for
    /// `bootstrap()`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: RaftConfig,
        log_store: Box<dyn LogStore>,
        quorum_state_store: Box<dyn QuorumStateStore>,
        snapshot_io: Box<dyn SnapshotIO>,
        transport_sender: Box<dyn TransportSender>,
        transport_receiver: Box<dyn TransportReceiver>,
        clock: Box<dyn Clock>,
        state_machine: S,
        listener: L,
    ) -> Result<Self, XraftError> {
        config
            .validate()
            .map_err(|reason| XraftError::InvalidConfig { reason })?;

        let node_id = config.node_id;
        let has_data = Self::has_existing_data(&*log_store, &*quorum_state_store, &*snapshot_io).await?;

        let state = NodeState::new_unattached(node_id);

        let mut node = Self {
            state,
            config,
            log_store,
            quorum_state_store,
            snapshot_io,
            transport_sender,
            transport_receiver,
            clock,
            state_machine,
            listener,
            bootstrapped: false,
        };

        if has_data {
            match node.recover().await {
                Ok(()) => { /* successfully recovered */ }
                Err(XraftError::RecoveryRequired) => {
                    // Data exists but is inconsistent (e.g. log entries without
                    // quorum-state or snapshot). Mark bootstrapped to block
                    // re-bootstrap, but stay Unattached for operator investigation.
                    node.bootstrapped = true;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(node)
    }

    /// Recovers node state from persisted data (log, quorum-state, snapshot).
    ///
    /// Restores the term, vote, cluster_id, voter set, and log boundaries from
    /// whatever data is available. Transitions to Follower with an election
    /// timer so the node can participate in elections.
    ///
    /// Safety: only scans log VotersRecords when quorum-state confirms the node
    /// participated in an election (implying data was committed). Full
    /// committed-vs-pending VotersRecord distinction is Stage 6.1 scope.
    async fn recover(&mut self) -> Result<(), XraftError> {
        // Restore from quorum-state if present
        let has_quorum_state = if let Some(qs) = self.quorum_state_store.load().await? {
            self.state.current_term = qs.current_term;
            self.state.voted_for = qs.voted_for;
            self.state.leader_id = qs.leader_id;
            self.state.cluster_id = qs.cluster_id;
            true
        } else {
            false
        };

        // Restore from snapshot if present — snapshot voters become the voter set
        // and snapshot boundaries set a floor for log_start_offset / high_watermark.
        let mut has_voters_from_snapshot = false;
        let mut snapshot_end: u64 = 0;
        if let Some(snap) = self.snapshot_io.load_latest().await? {
            self.state.voter_set = snap.metadata.voters.clone();
            has_voters_from_snapshot = true;
            snapshot_end = snap.metadata.last_included_offset + 1;
            self.state.log_start_offset = snapshot_end;
            self.state.high_watermark = snapshot_end;
            self.state_machine.restore(snap.app_snapshot)?;
        }

        // Restore log boundaries from the log store, taking the max with
        // snapshot-derived values so we never regress.
        let store_start = self.log_store.log_start_offset();
        let store_end = self.log_store.log_end_offset();
        if store_start > self.state.log_start_offset {
            self.state.log_start_offset = store_start;
        }
        self.state.log_end_offset = std::cmp::max(store_end, snapshot_end);

        // Scan log for the latest VotersRecord only if:
        // 1. snapshot didn't already provide voters, AND
        // 2. quorum-state exists (confirming the node participated in elections,
        //    so the VotersRecord is from a committed flow, not an uncommitted
        //    membership change tail).
        if !has_voters_from_snapshot && has_quorum_state && store_end > store_start {
            let entries = self
                .log_store
                .read(store_start, store_end)
                .await?;
            for entry in entries.iter().rev() {
                if entry.entry_type == EntryType::VotersRecord {
                    if let Ok(record) = bincode::deserialize::<VotersRecord>(&entry.payload) {
                        self.state.voter_set = record.voters;
                        break;
                    }
                }
            }
        }

        // Transition to Follower with election timer.
        // Even with incomplete data (nil cluster_id or empty voters), we still
        // transition to Follower to block re-bootstrap. The node will be limited:
        // nil cluster_id fails RPC fencing, empty voters prevents elections.
        // Full recovery validation is Stage 6.1 scope.
        self.state.role = Role::Follower;
        let timeout = self.clock.random_election_timeout();
        self.state.election_deadline = self.clock.now() + timeout;
        self.bootstrapped = true;

        Ok(())
    }

    /// First-time cluster formation per architecture §5.9.
    ///
    /// Stores `cluster_id` and voter set in memory, sets term=0 with no vote,
    /// transitions role from `Unattached` to `Follower`, and starts the
    /// election timer. No log entries are written and no quorum-state file
    /// is persisted — the quorum-state file is first created when the node
    /// votes during the first election.
    pub async fn bootstrap(
        &mut self,
        cluster_id: ClusterId,
        initial_voters: Vec<VoterInfo>,
    ) -> Result<(), XraftError> {
        // Guard: reject if already bootstrapped or recovered
        if self.bootstrapped {
            return Err(XraftError::AlreadyBootstrapped {
                reason: "node has already been bootstrapped or recovered".to_string(),
            });
        }
        if self.state.role != Role::Unattached {
            return Err(XraftError::AlreadyBootstrapped {
                reason: "node is not in Unattached state".to_string(),
            });
        }

        // Storage guard: re-check all three conditions
        let has_data = Self::has_existing_data(
            &*self.log_store,
            &*self.quorum_state_store,
            &*self.snapshot_io,
        )
        .await?;
        if has_data {
            return Err(XraftError::AlreadyBootstrapped {
                reason: "existing data detected (log, quorum-state, or snapshot)".to_string(),
            });
        }

        // Validate inputs
        Self::validate_bootstrap_config(self.state.node_id, &cluster_id, &initial_voters)?;

        // Apply bootstrap state in memory only
        self.state.cluster_id = cluster_id;
        self.state.voter_set = initial_voters;
        self.state.current_term = Term::ZERO;
        self.state.voted_for = None;
        self.state.leader_id = None;
        self.state.role = Role::Follower;
        self.state.votes_received.clear();
        self.state.pre_votes_received.clear();

        // Start election timer using the injected clock
        let timeout = self.clock.random_election_timeout();
        self.state.election_deadline = self.clock.now() + timeout;

        self.bootstrapped = true;

        Ok(())
    }

    /// Advances the node one tick: checks the election timer and fires an
    /// election timeout if the deadline has passed.
    ///
    /// Call this from the event loop or use `poll()` for async timer-driven
    /// operation.
    pub async fn tick(&mut self) -> Result<(), XraftError> {
        let now = self.clock.now();
        if (self.state.role == Role::Follower || self.state.role == Role::Candidate)
            && now >= self.state.election_deadline
        {
            self.handle_election_timeout().await?;
        }
        Ok(())
    }

    /// Async timer-driven election: sleeps until the election deadline using
    /// the injected clock, then fires `handle_election_timeout()`.
    ///
    /// This provides real timer-driven behaviour without a manual tick loop.
    /// Returns immediately if the node is not a Follower or Candidate.
    pub async fn poll(&mut self) -> Result<(), XraftError> {
        if self.state.role != Role::Follower && self.state.role != Role::Candidate {
            return Ok(());
        }
        self.clock.sleep_until(self.state.election_deadline).await;
        self.handle_election_timeout().await
    }

    /// Single iteration of the event loop: checks the election timer and
    /// processes one inbound RPC message from the transport receiver.
    ///
    /// This method combines timer-driven and message-driven processing into
    /// a single step, suitable for being called in a loop by the application.
    pub async fn step(&mut self) -> Result<(), XraftError> {
        // Check election timer
        self.tick().await?;

        // Try to receive and process one inbound message
        match self.transport_receiver.recv().await {
            Ok(envelope) => {
                self.handle_rpc(envelope).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut => {
                // No message available; continue
            }
            Err(e) => {
                return Err(XraftError::TransportError {
                    reason: format!("transport recv failed: {e}"),
                });
            }
        }

        Ok(())
    }

    /// Dispatches an inbound RPC envelope to the appropriate handler.
    pub async fn handle_rpc(&mut self, envelope: RpcEnvelope) -> Result<(), XraftError> {
        // After bootstrap, reject any message with a nil or mismatched cluster_id.
        if self.bootstrapped && !self.state.cluster_id.0.is_nil()
            && (envelope.cluster_id.0.is_nil() || self.state.cluster_id != envelope.cluster_id)
        {
            return Err(XraftError::InvalidClusterId);
        }

        match envelope.payload {
            RpcPayload::VoteRequest(req) => {
                let response = self.handle_vote_request(req).await?;
                let reply = RpcEnvelope {
                    cluster_id: self.state.cluster_id,
                    leader_epoch: Term::ZERO,
                    source: self.state.node_id,
                    payload: RpcPayload::VoteResponse(response),
                };
                self.transport_sender.send(envelope.source, reply).await
                    .map_err(|e| XraftError::TransportError {
                        reason: format!("failed to send VoteResponse: {e}"),
                    })?;
            }
            RpcPayload::VoteResponse(resp) => {
                self.handle_vote_response(envelope.source, resp).await?;
            }
            _ => {
                // Other RPC types handled in later stages
            }
        }

        Ok(())
    }

    /// Handles an election timeout: transitions to Candidate, self-votes,
    /// persists quorum-state (fsync-before-ack), broadcasts VoteRequests,
    /// and checks for immediate win.
    ///
    /// For a single-node cluster, the node wins the election immediately,
    /// transitions to Leader, and appends LeaderChangeMessage + VotersRecord.
    pub async fn handle_election_timeout(&mut self) -> Result<(), XraftError> {
        if self.state.role != Role::Follower && self.state.role != Role::Candidate {
            return Err(XraftError::InvalidElectionState {
                reason: format!(
                    "cannot start election from role {:?}",
                    self.state.role
                ),
            });
        }

        // Increment term and vote for self
        self.state.current_term = Term(self.state.current_term.0 + 1);
        self.state.voted_for = Some(self.state.node_id);
        self.state.role = Role::Candidate;
        self.state.leader_id = None;
        self.state.votes_received.clear();
        self.state.votes_received.insert(self.state.node_id);

        // Persist quorum-state before sending any messages (fsync-before-ack)
        let qs = QuorumState {
            current_term: self.state.current_term,
            voted_for: self.state.voted_for,
            leader_id: None,
            leader_epoch: Term::ZERO,
            cluster_id: self.state.cluster_id,
        };
        self.quorum_state_store.save(&qs).await?;

        // Check if we already have a quorum (single-node cluster)
        if self.has_quorum() {
            self.become_leader().await?;
        } else {
            // Broadcast VoteRequest to all other voters
            self.broadcast_vote_requests().await?;

            // Reset election timer for the next timeout
            let timeout = self.clock.random_election_timeout();
            self.state.election_deadline = self.clock.now() + timeout;
        }

        Ok(())
    }

    /// Handles an incoming VoteRequest from another candidate.
    ///
    /// Rejects candidates not in the voter set. Grants the vote if: the
    /// candidate's term >= our term, we haven't voted for someone else in
    /// this term, and the candidate's log is at least as up-to-date as ours.
    /// Durably persists quorum-state on any term advancement.
    pub async fn handle_vote_request(
        &mut self,
        request: VoteRequest,
    ) -> Result<VoteResponse, XraftError> {
        // Reject candidates not in the configured voter set
        if !self.state.voter_set.iter().any(|v| v.node_id == request.candidate_id) {
            return Ok(VoteResponse {
                term: self.state.current_term,
                vote_granted: false,
                is_pre_vote: request.is_pre_vote,
            });
        }

        // If the candidate's term is higher, step down and update our term.
        // Durably persist the new term even if we don't grant the vote.
        if request.term > self.state.current_term {
            self.state.current_term = request.term;
            self.state.voted_for = None;
            self.state.role = Role::Follower;
            self.state.leader_id = None;
            self.state.votes_received.clear();
            let timeout = self.clock.random_election_timeout();
            self.state.election_deadline = self.clock.now() + timeout;

            // Persist the higher term immediately (fsync-before-ack)
            let qs = QuorumState {
                current_term: self.state.current_term,
                voted_for: None,
                leader_id: None,
                leader_epoch: Term::ZERO,
                cluster_id: self.state.cluster_id,
            };
            self.quorum_state_store.save(&qs).await?;
        }

        // Reject if the candidate's term is stale
        if request.term < self.state.current_term {
            return Ok(VoteResponse {
                term: self.state.current_term,
                vote_granted: false,
                is_pre_vote: request.is_pre_vote,
            });
        }

        // Check if we can vote for this candidate
        let can_vote = match self.state.voted_for {
            None => true,
            Some(voted) => voted == request.candidate_id,
        };

        // Check log up-to-date: candidate's log must be at least as current
        let (our_last_offset, our_last_term) = self.last_log_info().await;
        let log_ok = request.last_log_term > our_last_term
            || (request.last_log_term == our_last_term
                && request.last_log_offset >= our_last_offset);

        let vote_granted = can_vote && log_ok;

        if vote_granted && !request.is_pre_vote {
            self.state.voted_for = Some(request.candidate_id);

            // Persist quorum-state (fsync-before-ack)
            let qs = QuorumState {
                current_term: self.state.current_term,
                voted_for: self.state.voted_for,
                leader_id: None,
                leader_epoch: Term::ZERO,
                cluster_id: self.state.cluster_id,
            };
            self.quorum_state_store.save(&qs).await?;

            // Reset election timer when granting a vote
            let timeout = self.clock.random_election_timeout();
            self.state.election_deadline = self.clock.now() + timeout;
        }

        Ok(VoteResponse {
            term: self.state.current_term,
            vote_granted,
            is_pre_vote: request.is_pre_vote,
        })
    }

    /// Broadcasts VoteRequest RPCs to all other voters in the cluster.
    async fn broadcast_vote_requests(&self) -> Result<(), XraftError> {
        let (last_log_offset, last_log_term) = self.last_log_info().await;

        let vote_request = VoteRequest {
            term: self.state.current_term,
            candidate_id: self.state.node_id,
            last_log_offset,
            last_log_term,
            is_pre_vote: false,
        };

        let envelope = RpcEnvelope {
            cluster_id: self.state.cluster_id,
            leader_epoch: Term::ZERO,
            source: self.state.node_id,
            payload: RpcPayload::VoteRequest(vote_request),
        };

        for voter in &self.state.voter_set {
            if voter.node_id != self.state.node_id {
                if let Err(e) = self.transport_sender.send(voter.node_id, envelope.clone()).await {
                    tracing::warn!(
                        target = %voter.node_id,
                        error = %e,
                        "failed to send VoteRequest"
                    );
                }
            }
        }

        Ok(())
    }

    /// Handles a VoteResponse from another node.
    ///
    /// Validates term, tracks the vote, and transitions to Leader if quorum
    /// is reached.
    pub async fn handle_vote_response(
        &mut self,
        from: NodeId,
        response: VoteResponse,
    ) -> Result<(), XraftError> {
        // Higher term: step down to Follower
        if response.term > self.state.current_term {
            self.state.current_term = response.term;
            self.state.voted_for = None;
            self.state.role = Role::Follower;
            self.state.leader_id = None;
            self.state.votes_received.clear();
            let timeout = self.clock.random_election_timeout();
            self.state.election_deadline = self.clock.now() + timeout;

            let qs = QuorumState {
                current_term: self.state.current_term,
                voted_for: None,
                leader_id: None,
                leader_epoch: Term::ZERO,
                cluster_id: self.state.cluster_id,
            };
            self.quorum_state_store.save(&qs).await?;
            return Ok(());
        }

        // Ignore if not Candidate or stale term
        if self.state.role != Role::Candidate || response.term != self.state.current_term {
            return Ok(());
        }

        // Ignore pre-vote responses here (only handle real votes)
        if response.is_pre_vote {
            return Ok(());
        }

        // Ignore vote from a node not in the voter set
        if !self.state.voter_set.iter().any(|v| v.node_id == from) {
            return Ok(());
        }

        if response.vote_granted {
            self.state.votes_received.insert(from);

            if self.has_quorum() {
                self.become_leader().await?;
            }
        }

        Ok(())
    }

    /// Returns the current public consensus state projection.
    pub fn read(&self) -> Result<ConsensusState, XraftError> {
        Ok(self.state.project())
    }

    /// Returns `true` if the node has been bootstrapped.
    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped
    }

    /// Returns the cluster ID assigned to this node.
    pub fn cluster_id(&self) -> ClusterId {
        self.state.cluster_id
    }

    /// Returns the node's own ID.
    pub fn node_id(&self) -> NodeId {
        self.state.node_id
    }

    /// Returns the current voter set (for testing/introspection).
    pub fn voter_set(&self) -> &[VoterInfo] {
        &self.state.voter_set
    }

    /// Returns the current election deadline (for testing).
    pub fn election_deadline(&self) -> std::time::Instant {
        self.state.election_deadline
    }

    /// Returns true if votes_received constitutes a majority of the voter set.
    fn has_quorum(&self) -> bool {
        let total = self.state.voter_set.len();
        if total == 0 {
            return false;
        }
        let needed = total / 2 + 1;
        self.state.votes_received.len() >= needed
    }

    /// Returns (last_log_offset, last_log_term) by reading the actual log.
    async fn last_log_info(&self) -> (u64, Term) {
        let end = self.log_store.log_end_offset();
        if end == 0 {
            return (0, Term::ZERO);
        }
        let last_offset = end - 1;
        match self.log_store.entry_at(last_offset).await {
            Ok(Some(entry)) => (last_offset, entry.term),
            _ => (last_offset, Term::ZERO),
        }
    }

    /// Returns true if the log contains at least one VotersRecord entry.
    async fn log_has_voters_record(&self) -> bool {
        let start = self.log_store.log_start_offset();
        let end = self.log_store.log_end_offset();
        if end <= start {
            return false;
        }
        match self.log_store.read(start, end).await {
            Ok(entries) => entries.iter().any(|e| e.entry_type == EntryType::VotersRecord),
            Err(_) => false,
        }
    }

    /// Returns a reference to the log store (for test assertions).
    pub fn log_store(&self) -> &dyn LogStore {
        &*self.log_store
    }

    /// Returns a reference to the quorum state store (for test assertions).
    pub fn quorum_state_store(&self) -> &dyn QuorumStateStore {
        &*self.quorum_state_store
    }

    /// Returns a reference to the snapshot IO (for test assertions).
    pub fn snapshot_io(&self) -> &dyn SnapshotIO {
        &*self.snapshot_io
    }

    /// Transitions the node from Candidate to Leader after winning an election.
    ///
    /// Always appends a LeaderChangeMessage. Appends a VotersRecord only if
    /// the log does not already contain one (i.e., during the initial bootstrap
    /// election). This approach is more robust than a node-local flag because
    /// it survives crash-recovery and works correctly when a different node
    /// wins a subsequent election after the original leader crashed.
    async fn become_leader(&mut self) -> Result<(), XraftError> {
        // Check before appending: does the log already have a VotersRecord?
        let needs_voters_record = !self.log_has_voters_record().await;

        // Append LeaderChangeMessage as the first entry of the new term
        let lcm_entry = LogEntry {
            offset: self.log_store.log_end_offset(),
            term: self.state.current_term,
            entry_type: EntryType::LeaderChangeMessage,
            payload: Bytes::new(),
        };
        self.log_store.append(&[lcm_entry]).await?;

        // Append VotersRecord only if none exists yet (initial bootstrap election)
        if needs_voters_record {
            let voters_record = VotersRecord {
                version: 1,
                voters: self.state.voter_set.clone(),
            };
            let payload = bincode::serialize(&voters_record)
                .map_err(std::io::Error::other)?;
            let vr_entry = LogEntry {
                offset: self.log_store.log_end_offset(),
                term: self.state.current_term,
                entry_type: EntryType::VotersRecord,
                payload: Bytes::from(payload),
            };
            if let Err(e) = self.log_store.append(&[vr_entry]).await {
                // Roll back the LCM entry to avoid partial state
                let lcm_offset = self.log_store.log_end_offset().saturating_sub(1);
                let _ = self.log_store.truncate_suffix(lcm_offset).await;
                return Err(e.into());
            }
        }

        // Only transition to Leader after all appends succeeded
        self.state.role = Role::Leader;
        self.state.leader_id = Some(self.state.node_id);

        // Update in-memory log boundaries
        self.state.log_end_offset = self.log_store.log_end_offset();

        // For single-node clusters, all entries are immediately committed
        // since the leader is the only voter (majority of 1).
        if self.state.voter_set.len() == 1 {
            self.state.high_watermark = self.state.log_end_offset;
        }

        // Notify listener of leader change
        self.listener
            .handle_leader_change(self.state.node_id, self.state.current_term);

        Ok(())
    }

    /// Checks whether any existing data is present in the stores.
    async fn has_existing_data(
        log_store: &dyn LogStore,
        quorum_state_store: &dyn QuorumStateStore,
        snapshot_io: &dyn SnapshotIO,
    ) -> Result<bool, XraftError> {
        if log_store.log_start_offset() != 0 || log_store.log_end_offset() != 0 {
            return Ok(true);
        }

        let qs = quorum_state_store.load().await?;
        if qs.is_some() {
            return Ok(true);
        }

        let snap = snapshot_io.load_latest().await?;
        if snap.is_some() {
            return Ok(true);
        }

        Ok(false)
    }

    /// Validates bootstrap configuration inputs.
    fn validate_bootstrap_config(
        node_id: NodeId,
        cluster_id: &ClusterId,
        initial_voters: &[VoterInfo],
    ) -> Result<(), XraftError> {
        if cluster_id.0.is_nil() {
            return Err(XraftError::InvalidBootstrapConfig {
                reason: "cluster_id must not be nil".to_string(),
            });
        }

        if initial_voters.is_empty() {
            return Err(XraftError::InvalidBootstrapConfig {
                reason: "initial_voters must not be empty".to_string(),
            });
        }

        let mut seen = HashSet::new();
        for voter in initial_voters {
            if !seen.insert(voter.node_id) {
                return Err(XraftError::InvalidBootstrapConfig {
                    reason: format!("duplicate node_id {} in initial_voters", voter.node_id),
                });
            }
        }

        if !initial_voters.iter().any(|v| v.node_id == node_id) {
            return Err(XraftError::InvalidBootstrapConfig {
                reason: format!(
                    "this node ({}) must be included in initial_voters",
                    node_id
                ),
            });
        }

        Ok(())
    }
}
