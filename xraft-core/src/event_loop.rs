use crate::error::XraftError;
use tokio::sync::{mpsc, watch};

use crate::config::RaftConfig;
use crate::consensus_state::{ConsensusState, Role};
use crate::election::ElectionManager;
use crate::io_action::{IoAction, IoActionBatch, IoStage};
use crate::listener::Listener;
use crate::node_state::NodeState;
use crate::quorum_state::QuorumState;
use crate::rpc::{RpcEnvelope, RpcPayload};
use crate::traits::{Clock, StateMachine};
use crate::types::Term;

/// Messages that can be sent to the event loop.
pub enum EventLoopMessage {
    Rpc(RpcEnvelope),
    Shutdown,
}

/// The single-threaded async event loop that drives all protocol state transitions.
#[allow(dead_code)]
pub struct EventLoop<S: StateMachine, L: Listener> {
    pub state: NodeState,
    pub config: RaftConfig,
    pub clock: Box<dyn Clock>,
    pub io_stage: IoStage,
    pub state_machine: S,
    pub listener: L,
    pub msg_rx: mpsc::Receiver<EventLoopMessage>,
    pub state_tx: watch::Sender<ConsensusState>,
}

impl<S: StateMachine, L: Listener> EventLoop<S, L> {
    /// Create a new EventLoop.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state: NodeState,
        config: RaftConfig,
        clock: Box<dyn Clock>,
        io_stage: IoStage,
        state_machine: S,
        listener: L,
        msg_rx: mpsc::Receiver<EventLoopMessage>,
        state_tx: watch::Sender<ConsensusState>,
    ) -> Self {
        Self {
            state,
            config,
            clock,
            io_stage,
            state_machine,
            listener,
            msg_rx,
            state_tx,
        }
    }

    /// Run the event loop until shutdown or fatal I/O error.
    /// I/O errors are propagated rather than logged-and-continued because
    /// state has already been mutated in-memory before I/O executes;
    /// continuing would leave inconsistent leadership state.
    pub async fn run(&mut self) -> Result<(), XraftError> {
        loop {
            let now = self.clock.now();

            // Check timers before waiting for messages
            if self.state.role == Role::Leader {
                if now >= self.state.check_quorum_deadline {
                    self.handle_check_quorum().await?;
                }
            } else if (self.state.role == Role::Follower || self.state.role == Role::Candidate)
                && now >= self.state.election_deadline
            {
                self.handle_election_timeout().await?;
            }

            // Calculate next timer deadline
            let next_deadline = if self.state.role == Role::Leader {
                self.state.check_quorum_deadline
            } else {
                self.state.election_deadline
            };

            // Wait for a message or the next timer
            tokio::select! {
                msg = self.msg_rx.recv() => {
                    match msg {
                        Some(EventLoopMessage::Rpc(envelope)) => {
                            self.handle_rpc(envelope).await?;
                        }
                        Some(EventLoopMessage::Shutdown) | None => {
                            self.listener.begin_shutdown();
                            break;
                        }
                    }
                }
                _ = self.clock.sleep_until(next_deadline) => {
                    // Timer fired, will be handled at top of loop
                }
            }
        }
        Ok(())
    }

    /// Handle the Check Quorum timer: verify majority of voters have
    /// sent a Fetch within the `check_quorum_interval` window (the actual
    /// randomized election-timeout that was set when this node became leader).
    pub async fn handle_check_quorum(&mut self) -> Result<(), XraftError> {
        let now = self.clock.now();
        let interval = self.state.check_quorum_interval;

        if self.state.check_quorum(now, interval) {
            tracing::debug!(
                node = %self.state.node_id,
                term = %self.state.current_term,
                "check quorum passed"
            );
            self.state.check_quorum_deadline = now + interval;
        } else {
            tracing::warn!(
                node = %self.state.node_id,
                term = %self.state.current_term,
                "check quorum failed: stepping down to Follower"
            );
            // Preserve voted_for from the leader phase (Some(self.node_id)).
            // Clearing it would allow this node to grant a same-term
            // VoteRequest, violating Raft's at-most-one-leader-per-term
            // safety property.
            let preserved_voted_for = self.state.voted_for;
            let deadline = now + self.clock.random_election_timeout();
            self.state.become_follower(self.state.current_term, None, deadline);
            self.state.voted_for = preserved_voted_for;

            let mut batch = IoActionBatch::new();
            batch.push(IoAction::PersistQuorumState(QuorumState {
                current_term: self.state.current_term,
                voted_for: preserved_voted_for,
                leader_id: None,
                leader_epoch: Term(0),
            }));

            // Publish state BEFORE I/O (architecture §3.2 step 4 before step 5)
            let _ = self.state_tx.send(self.state.project());

            self.io_stage.execute(&batch).await?;
            return Ok(());
        }

        // Publish updated state BEFORE any I/O
        let _ = self.state_tx.send(self.state.project());
        Ok(())
    }

    /// Handle election timeout expiry.
    pub async fn handle_election_timeout(&mut self) -> Result<(), XraftError> {
        if !self.state.is_voter() {
            let deadline = self.clock.now() + self.clock.random_election_timeout();
            self.state.election_deadline = deadline;
            return Ok(());
        }

        let mut batch = IoActionBatch::new();
        ElectionManager::start_election(&mut self.state, &*self.clock, &self.config, &mut batch);

        // Check if single-node cluster → became leader immediately
        if self.state.voter_count() == 1 {
            self.state.become_leader(self.clock.now(), self.clock.random_election_timeout());
            self.on_become_leader(&mut batch);
        }

        // Publish state BEFORE I/O (architecture §3.2 step 4 before step 5)
        let _ = self.state_tx.send(self.state.project());

        self.io_stage.execute(&batch).await?;
        Ok(())
    }

    /// Handle an incoming RPC message.
    /// Validates cluster_id, applies higher-term step-down (except pre-vote),
    /// and dispatches to payload-specific handlers.
    pub async fn handle_rpc(&mut self, envelope: RpcEnvelope) -> Result<(), XraftError> {
        // ── Cluster-id fencing ──────────────────────────────────────
        if envelope.cluster_id != self.state.cluster_id {
            tracing::warn!(
                node = %self.state.node_id,
                expected = ?self.state.cluster_id,
                received = ?envelope.cluster_id,
                source = %envelope.source,
                "rejecting RPC: cluster_id mismatch"
            );
            return Ok(());
        }

        let source = envelope.source;
        let mut batch = IoActionBatch::new();

        // ── Determine if this is a pre-vote message ─────────────────
        let is_pre_vote = match &envelope.payload {
            RpcPayload::VoteRequest(req) => req.is_pre_vote,
            RpcPayload::VoteResponse(resp) => resp.is_pre_vote,
            _ => false,
        };

        // ── Effective term for higher-term step-down ────────────────
        let effective_term = match &envelope.payload {
            RpcPayload::VoteRequest(req) => req.term,
            RpcPayload::VoteResponse(resp) => resp.term,
            RpcPayload::FetchRequest(_) => envelope.leader_epoch,
            RpcPayload::FetchResponse(resp) => resp.leader_epoch,
            RpcPayload::FetchSnapshotRequest(_) => envelope.leader_epoch,
            RpcPayload::FetchSnapshotResponse(_) => envelope.leader_epoch,
        };

        // Pre-vote messages must NOT cause term advancement or state changes.
        // Only real (non-pre-vote) messages trigger higher-term step-down.
        let stepped_down = if is_pre_vote {
            false
        } else {
            let sd = ElectionManager::maybe_step_down_on_higher_term(
                &mut self.state,
                effective_term,
                source,
                &*self.clock,
                &self.config,
            );
            if sd {
                batch.push(IoAction::PersistQuorumState(QuorumState {
                    current_term: self.state.current_term,
                    voted_for: None,
                    leader_id: None,
                    leader_epoch: Term(0),
                }));
            }
            sd
        };

        // ── Payload-specific handling ───────────────────────────────
        match envelope.payload {
            RpcPayload::VoteRequest(ref req) => {
                if req.is_pre_vote {
                    // Pre-vote: read-only evaluation, no state mutation
                    ElectionManager::handle_pre_vote_request(
                        &self.state,
                        req,
                        &mut batch,
                    );
                } else {
                    // Real vote: may mutate term/voted_for
                    ElectionManager::handle_vote_request(
                        &mut self.state,
                        req,
                        &*self.clock,
                        &self.config,
                        &mut batch,
                    );
                }
            }
            RpcPayload::VoteResponse(ref resp) => {
                // Skip pre-vote responses for real vote counting
                if resp.is_pre_vote {
                    // Pre-vote response: record in pre_votes_received (future pre-vote phase)
                    if resp.vote_granted && resp.term == self.state.current_term {
                        self.state.pre_votes_received.insert(source);
                    }
                } else if !stepped_down
                    && self.state.role == Role::Candidate
                    && resp.vote_granted
                    && resp.term == self.state.current_term
                {
                    // Reject votes from nodes not in the voter set
                    if !self.state.is_in_voter_set(source) {
                        tracing::warn!(
                            node = %self.state.node_id,
                            source = %source,
                            "ignoring VoteResponse from non-voter"
                        );
                    } else {
                        let has_majority = ElectionManager::record_vote(
                            &mut self.state,
                            source,
                        );
                        if has_majority {
                            self.state.become_leader(
                                self.clock.now(),
                                self.clock.random_election_timeout(),
                            );
                            self.on_become_leader(&mut batch);
                        }
                    }
                }
            }
            RpcPayload::FetchRequest(ref req) => {
                if self.state.role == Role::Leader {
                    self.state.record_fetch(
                        source,
                        req.fetch_offset,
                        self.clock.now(),
                    );
                }
                // Full fetch response handling is in replication phase (Stage 5)
            }
            RpcPayload::FetchResponse(_) => {
                // Full fetch response handling is in replication phase (Stage 5)
            }
            RpcPayload::FetchSnapshotRequest(_) | RpcPayload::FetchSnapshotResponse(_) => {
                // Snapshot transfer handling is in snapshot phase (Stage 6).
            }
        }

        // Publish state BEFORE I/O (architecture §3.2 step 4 before step 5)
        let _ = self.state_tx.send(self.state.project());

        self.io_stage.execute(&batch).await?;
        Ok(())
    }

    /// Called when this node transitions to Leader.
    /// Appends LeaderChangeMessage and notifies listener synchronously.
    fn on_become_leader(&mut self, batch: &mut IoActionBatch) {
        tracing::info!(
            node = %self.state.node_id,
            term = %self.state.current_term,
            "became leader"
        );

        batch.push(IoAction::PersistQuorumState(QuorumState {
            current_term: self.state.current_term,
            voted_for: self.state.voted_for, // preserve voted_for=Some(self) for leader safety
            leader_id: Some(self.state.node_id),
            leader_epoch: self.state.current_term,
        }));

        // Append LeaderChangeMessage control record
        ElectionManager::append_leader_change_message(&mut self.state, batch);

        // Notify listener synchronously (before I/O, per architecture §4.1)
        ElectionManager::notify_leader_change(
            &mut self.listener,
            self.state.node_id,
            self.state.current_term,
        );
    }
}
