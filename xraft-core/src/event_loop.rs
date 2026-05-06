//! EventLoopDriver — processes inbound messages following the architecture's
//! prescribed three-phase commit notification and IoAction production.
//!
//! Per architecture §4.1, the event loop processes each message in order:
//! 1. Mutate NodeState
//! 2. Invoke application callbacks synchronously (SM::apply, Listener, DCQ)
//! 3. Produce IoActionBatch for execution by IoStage
//!
//! The EventLoopDriver encapsulates this pipeline, separating consensus state
//! mutation from I/O execution.

use crate::config::RaftConfig;
use crate::deferred_completion::DeferredCompletionQueue;
use crate::io_action::{IoAction, IoActionBatch};
use crate::listener::Listener;
use crate::log_entry::EntryType;
use crate::raft_node::RaftNode;
use crate::rpc::*;
use crate::traits::{QuorumState, StateMachine};
use crate::types::*;
use tokio::sync::oneshot;

/// Inbound messages the event loop processes.
#[derive(Debug)]
pub enum InboundMessage {
    /// A proposal from a client.
    Propose(AppRecord),
    /// A FetchRequest from a follower (leader-side).
    FetchRequest { from: NodeId, request: FetchRequest },
    /// A FetchResponse from the leader (follower-side).
    FetchResponse {
        from: NodeId,
        response: FetchResponse,
    },
    /// A VoteRequest from a candidate.
    VoteRequest { from: NodeId, request: VoteRequest },
    /// A VoteResponse from a voter.
    VoteResponse {
        from: NodeId,
        response: VoteResponse,
    },
}

/// The EventLoopDriver ties together NodeState, StateMachine, Listener,
/// and DeferredCompletionQueue in the architecture's prescribed processing
/// order. It produces IoActionBatch values for the IoStage to execute.
pub struct EventLoopDriver<S: StateMachine, L: Listener> {
    pub node: RaftNode,
    pub state_machine: S,
    pub listener: L,
    pub completion_queue: DeferredCompletionQueue,
    pub cluster_id: String,
    #[allow(dead_code)]
    config: RaftConfig,
}

impl<S: StateMachine, L: Listener> EventLoopDriver<S, L> {
    pub fn new(
        node: RaftNode,
        state_machine: S,
        listener: L,
        cluster_id: String,
        config: RaftConfig,
    ) -> Self {
        Self {
            node,
            state_machine,
            listener,
            completion_queue: DeferredCompletionQueue::new(),
            cluster_id,
            config,
        }
    }

    /// Process an inbound message, invoke callbacks, and produce an IoActionBatch.
    ///
    /// Returns `(IoActionBatch, Option<oneshot::Receiver<u64>>)`:
    /// - The batch contains I/O actions to execute through the IoStage
    /// - For Propose messages, returns a completion receiver that fires when committed
    pub fn process(
        &mut self,
        msg: InboundMessage,
        now_ms: u64,
    ) -> (IoActionBatch, Option<oneshot::Receiver<u64>>) {
        let mut batch = IoActionBatch::new();
        let prev_hw = self.node.high_watermark();
        let prev_term = self.node.term();
        let prev_leader = self.node.state.leader_id;
        let mut completion_rx = None;

        match msg {
            InboundMessage::Propose(record) => {
                if let Some(offset) = self.node.propose(&record) {
                    // Enqueue for deferred completion
                    let rx = self.completion_queue.enqueue(offset);
                    completion_rx = Some(rx);

                    // Produce AppendLog IoAction for the new entry
                    let entry = self.node.log().last().unwrap().clone();
                    batch.push(IoAction::AppendLog(vec![entry]));
                }
            }

            InboundMessage::FetchRequest { from, request } => {
                let resp = self.node.handle_fetch_request(&request, now_ms);
                let envelope = RpcEnvelope {
                    cluster_id: self.cluster_id.clone(),
                    source: self.node.node_id(),
                    leader_epoch: resp.leader_epoch,
                    payload: RpcPayload::FetchResponse(resp),
                };
                batch.push(IoAction::SendRpc(from, envelope));
            }

            InboundMessage::FetchResponse { from: _, response } => {
                let node_leo_before = self.node.log_end_offset();
                self.node
                    .handle_fetch_response(&response, now_ms);

                // If log was truncated, produce TruncateSuffix IoAction
                let node_leo_after = self.node.log_end_offset();
                if node_leo_after < node_leo_before {
                    batch.push(IoAction::TruncateSuffix(node_leo_after));
                }

                // If new entries were appended, produce AppendLog IoAction
                if node_leo_after > node_leo_before {
                    let new_entries =
                        self.node.state.read_entries(node_leo_before, node_leo_after);
                    if !new_entries.is_empty() {
                        batch.push(IoAction::AppendLog(new_entries));
                    }
                }
            }

            InboundMessage::VoteRequest { from, request } => {
                let resp = self.node.handle_vote_request(&request, now_ms);
                let envelope = RpcEnvelope {
                    cluster_id: self.cluster_id.clone(),
                    source: self.node.node_id(),
                    leader_epoch: resp.term.0,
                    payload: RpcPayload::VoteResponse(resp),
                };
                batch.push(IoAction::SendRpc(from, envelope));
            }

            InboundMessage::VoteResponse { from, response } => {
                let won = self.node.handle_vote_response(&response, from, now_ms);
                if won {
                    // Leader appended a LeaderChangeMessage — produce AppendLog
                    let entry = self.node.log().last().unwrap().clone();
                    batch.push(IoAction::AppendLog(vec![entry]));
                }
            }
        }

        // Three-phase commit notification per architecture §4.1:
        // If HW advanced, invoke callbacks in fixed order.
        let new_hw = self.node.high_watermark();
        if new_hw > prev_hw {
            // Phase 1: StateMachine::apply — once per newly committed command entry
            let mut committed_records = Vec::new();
            for entry in self.node.log() {
                if entry.offset >= prev_hw && entry.offset < new_hw {
                    if entry.entry_type == EntryType::Command {
                        let record = AppRecord {
                            data: entry.data.clone(),
                        };
                        let _ = self.state_machine.apply(entry.offset, &record);
                        committed_records.push(record);
                    }
                }
            }
            self.node.state.mark_applied(new_hw);

            // Phase 2: Listener::handle_commit — batch of committed records
            if !committed_records.is_empty() {
                self.listener.handle_commit(&committed_records);
            }

            // Phase 3: DeferredCompletionQueue::complete — resolve client futures
            self.completion_queue.complete(new_hw);
        }

        // Detect leadership changes
        let new_term = self.node.term();
        let new_leader = self.node.state.leader_id;
        if new_term != prev_term || new_leader != prev_leader {
            self.listener.handle_leader_change(new_leader, new_term);
        }

        // Persist quorum state if term or vote changed
        if new_term != prev_term || self.node.state.voted_for.is_some() {
            let qs = QuorumState {
                current_term: self.node.term(),
                voted_for: self.node.state.voted_for,
                leader_epoch: self.node.term().0,
            };
            batch.push(IoAction::PersistQuorumState(qs));
        }

        (batch, completion_rx)
    }

    /// Start an election: transition to candidate state and produce
    /// VoteRequest IoActions for all peers.
    pub fn start_election(&mut self, now_ms: u64) -> IoActionBatch {
        let mut batch = IoActionBatch::new();

        self.node.start_election(now_ms);

        let term = self.node.term();
        let last_log_offset = self.node.state.last_log_offset();
        let last_log_term = self.node.state.last_log_term();

        let vote_req = VoteRequest {
            term,
            candidate_id: self.node.node_id(),
            last_log_offset,
            last_log_term,
            is_pre_vote: false,
        };

        // Send VoteRequest to each peer
        for voter in &self.node.state.voter_set.clone() {
            if voter.node_id != self.node.node_id() {
                let envelope = RpcEnvelope {
                    cluster_id: self.cluster_id.clone(),
                    source: self.node.node_id(),
                    leader_epoch: term.0,
                    payload: RpcPayload::VoteRequest(vote_req.clone()),
                };
                batch.push(IoAction::SendRpc(voter.node_id, envelope));
            }
        }

        // Persist the updated quorum state (new term, voted for self)
        let qs = QuorumState {
            current_term: self.node.term(),
            voted_for: self.node.state.voted_for,
            leader_epoch: self.node.term().0,
        };
        batch.push(IoAction::PersistQuorumState(qs));

        batch
    }

    /// Build a FetchRequest for this node (follower-side) to send to the leader.
    pub fn build_fetch_request(&self) -> FetchRequest {
        FetchRequest {
            replica_id: self.node.node_id(),
            fetch_offset: self.node.log_end_offset(),
            last_fetched_epoch: self.node.state.last_log_term().0,
            max_bytes: 1024 * 1024,
        }
    }
}
