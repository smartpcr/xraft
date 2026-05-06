use bytes::Bytes;

use crate::config::RaftConfig;
use crate::consensus_state::Role;
use crate::io_action::{IoAction, IoActionBatch};
use crate::listener::Listener;
use crate::log_entry::{EntryType, LogEntry};
use crate::node_state::NodeState;
use crate::quorum_state::QuorumState;
use crate::rpc::{RpcEnvelope, RpcPayload, VoteRequest, VoteResponse};
use crate::traits::Clock;
use crate::types::{NodeId, Term};

/// Handles election logic: vote requests, vote responses, term management,
/// leader step-down on higher term, and LeaderChangeMessage append.
pub struct ElectionManager;

impl ElectionManager {
    /// Handle receiving a message with a potentially higher term.
    /// If the message term is higher than current term, step down to Follower.
    /// Returns true if a step-down occurred.
    pub fn maybe_step_down_on_higher_term(
        state: &mut NodeState,
        message_term: Term,
        source: NodeId,
        clock: &dyn Clock,
        _config: &RaftConfig,
    ) -> bool {
        if message_term > state.current_term {
            tracing::info!(
                node = %state.node_id,
                current_term = %state.current_term,
                message_term = %message_term,
                source = %source,
                "stepping down: received message with higher term"
            );
            let deadline = clock.now() + clock.random_election_timeout();
            state.become_follower(message_term, None, deadline);
            true
        } else {
            false
        }
    }

    /// Handle election timeout expiry: transition to Candidate, increment term,
    /// vote for self, and produce VoteRequest broadcasts.
    pub fn start_election(
        state: &mut NodeState,
        clock: &dyn Clock,
        _config: &RaftConfig,
        batch: &mut IoActionBatch,
    ) {
        let deadline = clock.now() + clock.random_election_timeout();
        state.become_candidate(deadline);

        tracing::info!(
            node = %state.node_id,
            term = %state.current_term,
            "starting election"
        );

        // Persist the vote before sending any messages
        batch.push(IoAction::PersistQuorumState(QuorumState {
            current_term: state.current_term,
            voted_for: state.voted_for,
            leader_id: None,
            leader_epoch: Term(0),
        }));

        // Broadcast VoteRequest to all other voters
        let last_log_offset = if state.log_end_offset > 0 {
            state.log_end_offset - 1
        } else {
            0
        };

        for voter in &state.voter_set {
            if voter.node_id != state.node_id {
                let vote_req = VoteRequest {
                    term: state.current_term,
                    candidate_id: state.node_id,
                    last_log_offset,
                    last_log_term: state.last_log_term,
                    is_pre_vote: false,
                };
                batch.push(IoAction::SendRpc(
                    voter.node_id,
                    RpcEnvelope {
                        cluster_id: state.cluster_id,
                        leader_epoch: Term(0),
                        source: state.node_id,
                        payload: RpcPayload::VoteRequest(vote_req),
                    },
                ));
            }
        }

        // Check if single-node cluster → immediately become leader
        if state.voter_count() == 1 && state.is_voter() {
            // sole voter wins immediately
        }
    }

    /// Handle an incoming VoteRequest (real vote, not pre-vote).
    /// Pre-vote requests must be routed to `handle_pre_vote_request` instead.
    pub fn handle_vote_request(
        state: &mut NodeState,
        req: &VoteRequest,
        clock: &dyn Clock,
        config: &RaftConfig,
        batch: &mut IoActionBatch,
    ) {
        debug_assert!(!req.is_pre_vote, "pre-vote must not enter handle_vote_request");

        // Step down if higher term (safe: real votes only)
        Self::maybe_step_down_on_higher_term(state, req.term, req.candidate_id, clock, config);

        // Leaders must never grant same-term votes — even though voted_for is
        // already set to self (preventing grants), this explicit guard ensures
        // leader safety regardless of voted_for state.
        // Merge the two rejection conditions to satisfy clippy::if_same_then_else.
        let vote_granted = if state.role == Role::Leader
            || req.term < state.current_term
            || (state.voted_for.is_some() && state.voted_for != Some(req.candidate_id))
        {
            false
        } else {
            let our_last_offset = if state.log_end_offset > 0 {
                state.log_end_offset - 1
            } else {
                0
            };
            let log_ok = req.last_log_term > state.last_log_term
                || (req.last_log_term == state.last_log_term
                    && req.last_log_offset >= our_last_offset)
                || state.log_end_offset == 0;
            if log_ok {
                state.voted_for = Some(req.candidate_id);
                state.election_deadline = clock.now() + clock.random_election_timeout();
                true
            } else {
                false
            }
        };

        if vote_granted {
            batch.push(IoAction::PersistQuorumState(QuorumState {
                current_term: state.current_term,
                voted_for: state.voted_for,
                leader_id: state.leader_id,
                leader_epoch: Term(0),
            }));
        }

        batch.push(IoAction::SendRpc(
            req.candidate_id,
            RpcEnvelope {
                cluster_id: state.cluster_id,
                leader_epoch: Term(0),
                source: state.node_id,
                payload: RpcPayload::VoteResponse(VoteResponse {
                    term: state.current_term,
                    vote_granted,
                    is_pre_vote: false,
                }),
            },
        ));
    }

    /// Handle a pre-vote request. No term advancement, no voted_for mutation,
    /// no persisted state changes (Stage 4.3 no-mutation rule).
    pub fn handle_pre_vote_request(
        state: &NodeState,
        req: &VoteRequest,
        batch: &mut IoActionBatch,
    ) {
        debug_assert!(req.is_pre_vote, "real vote must not enter handle_pre_vote_request");

        // A pre-vote is granted if the candidate's term is at least as high
        // as ours and its log is at least as up-to-date. We never change
        // current_term or voted_for.
        let would_grant = if req.term < state.current_term {
            false
        } else {
            let our_last_offset = if state.log_end_offset > 0 {
                state.log_end_offset - 1
            } else {
                0
            };
            req.last_log_term > state.last_log_term
                || (req.last_log_term == state.last_log_term
                    && req.last_log_offset >= our_last_offset)
                || state.log_end_offset == 0
        };

        batch.push(IoAction::SendRpc(
            req.candidate_id,
            RpcEnvelope {
                cluster_id: state.cluster_id,
                leader_epoch: Term(0),
                source: state.node_id,
                payload: RpcPayload::VoteResponse(VoteResponse {
                    term: state.current_term,
                    vote_granted: would_grant,
                    is_pre_vote: true,
                }),
            },
        ));
    }

    /// Handle an incoming VoteResponse. If majority reached, transition to Leader.
    /// Returns true if the node became leader (caller should append LeaderChangeMessage).
    #[allow(dead_code)]
    pub fn handle_vote_response(
        state: &mut NodeState,
        resp: &VoteResponse,
        clock: &dyn Clock,
        _config: &RaftConfig,
    ) -> bool {
        // Step down if higher term
        if resp.term > state.current_term {
            let deadline = clock.now() + clock.random_election_timeout();
            state.become_follower(resp.term, None, deadline);
            return false;
        }

        // Only process if we're still a candidate for this term
        if state.role != Role::Candidate || resp.term != state.current_term {
            return false;
        }

        if resp.vote_granted {
            // We don't know the source from VoteResponse alone,
            // but the event loop knows it from the RpcEnvelope.source
            // This is handled in event_loop where we have the full envelope.
            return false;
        }

        false
    }

    /// Record a vote from a specific node. Returns true if a majority has been
    /// reached and the caller should initiate leader transition.
    /// Does NOT call `become_leader` — the event loop owns the full transition
    /// (including durable I/O) and must call it after this returns true.
    pub fn record_vote(
        state: &mut NodeState,
        voter: NodeId,
    ) -> bool {
        if state.role != Role::Candidate {
            return false;
        }

        state.votes_received.insert(voter);
        state.votes_received.len() >= state.majority()
    }

    /// Append a LeaderChangeMessage control record when a new leader is elected.
    /// This establishes commit state for the new term and updates in-memory
    /// last_log_term so subsequent log-freshness / election checks are correct.
    pub fn append_leader_change_message(
        state: &mut NodeState,
        batch: &mut IoActionBatch,
    ) -> LogEntry {
        let entry = LogEntry {
            offset: state.log_end_offset,
            term: state.current_term,
            entry_type: EntryType::LeaderChangeMessage,
            payload: Bytes::new(), // no-op control record
        };

        state.log_end_offset += 1;
        // Keep in-memory last_log_term in sync with the log tail.
        state.update_last_log_term(state.current_term);
        batch.push(IoAction::AppendLog(vec![entry.clone()]));

        tracing::info!(
            node = %state.node_id,
            term = %state.current_term,
            offset = entry.offset,
            "appended LeaderChangeMessage"
        );

        entry
    }

    /// Notify the listener of a leadership change.
    pub fn notify_leader_change<L: Listener>(
        listener: &mut L,
        leader_id: NodeId,
        term: Term,
    ) {
        listener.handle_leader_change(leader_id, term);
    }
}
