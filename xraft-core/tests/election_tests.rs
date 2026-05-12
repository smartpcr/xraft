use std::collections::HashSet;

use xraft_core::consensus_state::Role;
use xraft_core::election::{
    ElectionAction, ElectionDriver, ElectionEvent, ElectionExecutor,
    ElectionManager,
};
use xraft_core::node_state::{ElectionPhase, NodeState};
use xraft_core::quorum_state::InMemoryQuorumStateStore;
use xraft_core::rpc::{FetchResponse, VoteRequest, VoteResponse};
use xraft_core::types::{ClusterId, NodeId, Offset, Term};
use xraft_core::voter::VoterInfo;

// ── Helpers ─────────────────────────────────────────────────────────

fn cluster_id() -> ClusterId {
    ClusterId(uuid::Uuid::nil())
}

fn three_voters() -> Vec<VoterInfo> {
    vec![
        VoterInfo { node_id: NodeId(1), endpoint: "n1".into() },
        VoterInfo { node_id: NodeId(2), endpoint: "n2".into() },
        VoterInfo { node_id: NodeId(3), endpoint: "n3".into() },
    ]
}

fn make_state(id: u64) -> NodeState {
    NodeState::new(NodeId(id), cluster_id(), three_voters())
}

const ELECTION_TIMEOUT_MS: u64 = 150;

// ═══════════════════════════════════════════════════════════════════
// Pre-Vote initiation
// ═══════════════════════════════════════════════════════════════════

#[test]
fn start_pre_vote_sends_requests_to_other_voters() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);

    let action = ElectionManager::start_pre_vote(&mut state);

    // Must be in pre-vote phase.
    assert_eq!(state.election_phase, ElectionPhase::PreVote);

    // Self pre-vote is recorded.
    assert!(state.pre_votes_received.contains(&NodeId(1)));

    // Action sends to other two voters.
    match action {
        ElectionAction::SendPreVoteRequests(targets) => {
            assert_eq!(targets.len(), 2);
            let ids: HashSet<NodeId> = targets.iter().map(|(id, _)| *id).collect();
            assert!(ids.contains(&NodeId(2)));
            assert!(ids.contains(&NodeId(3)));

            // Each request uses prospective term = current + 1.
            for (_, req) in &targets {
                assert!(req.is_pre_vote);
                assert_eq!(req.term, Term(6));
                assert_eq!(req.candidate_id, NodeId(1));
                assert_eq!(req.last_log_offset, Offset(10));
                assert_eq!(req.last_log_term, Term(5));
            }
        }
        other => panic!("expected SendPreVoteRequests, got {other:?}"),
    }
}

#[test]
fn start_pre_vote_does_not_mutate_persistent_state() {
    let mut state = make_state(1);
    state.current_term = Term(3);
    state.voted_for = None;

    let _action = ElectionManager::start_pre_vote(&mut state);

    // Term must NOT be incremented.
    assert_eq!(state.current_term, Term(3));
    // voted_for must NOT be set.
    assert_eq!(state.voted_for, None);
    // Role must remain Follower (not Candidate).
    assert_eq!(state.role, Role::Follower);
}

#[test]
fn start_pre_vote_keeps_role_unchanged() {
    let mut state = make_state(1);
    state.role = Role::Follower;

    let _action = ElectionManager::start_pre_vote(&mut state);

    // During pre-vote, role should stay Follower — the node should still
    // accept leader contact. Only election_phase marks the pre-vote.
    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.election_phase, ElectionPhase::PreVote);
}

// ═══════════════════════════════════════════════════════════════════
// Pre-Vote success scenario
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_vote_success_leads_to_start_real_election() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);

    // Start pre-vote.
    let _action = ElectionManager::start_pre_vote(&mut state);

    // N2 grants pre-vote.
    let resp = VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);

    // Majority reached (self + N2 = 2 out of 3).
    assert_eq!(action, ElectionAction::StartRealElection);
}

#[test]
fn pre_vote_success_then_real_election_increments_term() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);

    // Start pre-vote.
    let _action = ElectionManager::start_pre_vote(&mut state);

    // N2 grants → triggers StartRealElection.
    let resp = VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true };
    let _action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);

    // Now start real election.
    let action = ElectionManager::start_real_election(&mut state);

    // Term incremented.
    assert_eq!(state.current_term, Term(6));
    // voted_for set to self.
    assert_eq!(state.voted_for, Some(NodeId(1)));
    // Role becomes Candidate.
    assert_eq!(state.role, Role::Candidate);
    // Phase is now Election.
    assert_eq!(state.election_phase, ElectionPhase::Election);

    // Action requires persistence before sending.
    match action {
        ElectionAction::PersistAndSendVoteRequests { term, voted_for, requests } => {
            assert_eq!(term, Term(6));
            assert_eq!(voted_for, NodeId(1));
            assert_eq!(requests.len(), 2);
            for (_, req) in &requests {
                assert!(!req.is_pre_vote);
                assert_eq!(req.term, Term(6));
            }
        }
        other => panic!("expected PersistAndSendVoteRequests, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Pre-Vote prevents disruptive election
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_vote_rejected_when_follower_heard_from_leader_recently() {
    // N2 recently heard from leader N1.
    let mut n2_state = make_state(2);
    n2_state.current_term = Term(5);
    n2_state.record_leader_contact(1000, NodeId(1), Term(5)); // heard at t=1000

    // N3 sends pre-vote at t=1050 (within election timeout of 150ms).
    let request = VoteRequest {
        term: Term(6), // prospective term
        candidate_id: NodeId(3),
        last_log_offset: Offset(10),
        last_log_term: Term(5),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut n2_state,
        &request,
        1050, // now_ms — only 50ms since last leader contact
        ELECTION_TIMEOUT_MS,
    );

    // N2 rejects: recently heard from leader.
    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(!resp.vote_granted);
            assert!(resp.is_pre_vote);
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }

    // N2's state is unchanged — no persistence needed.
    assert_eq!(n2_state.current_term, Term(5));
    assert_eq!(n2_state.voted_for, None);
}

#[test]
fn pre_vote_rejected_prevents_partitioned_node_from_disrupting() {
    // Full scenario: N3 partitioned, tries pre-vote, N2 rejects, N3 cannot
    // proceed to real election.
    let mut n3_state = make_state(3);
    n3_state.current_term = Term(5);
    n3_state.last_log_term = Term(5);
    n3_state.log_end_offset = Offset(10);

    // N3 starts pre-vote.
    let _action = ElectionManager::start_pre_vote(&mut n3_state);
    assert_eq!(n3_state.election_phase, ElectionPhase::PreVote);

    // N2 rejects (it heard from leader recently).
    let reject = VoteResponse { term: Term(5), vote_granted: false, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut n3_state, NodeId(2), &reject);
    assert_eq!(action, ElectionAction::None);

    // N1 is partitioned — no response arrives.
    // N3 does NOT have a majority → cannot start real election.
    assert_eq!(n3_state.pre_votes_received.len(), 1); // only self
    assert_eq!(n3_state.current_term, Term(5)); // term unchanged
}

#[test]
fn pre_vote_granted_when_no_recent_leader_contact() {
    // N2 hasn't heard from any leader.
    let mut n2_state = make_state(2);
    n2_state.current_term = Term(5);
    n2_state.last_leader_contact_ms = None;

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(1),
        last_log_offset: Offset(10),
        last_log_term: Term(5),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut n2_state,
        &request,
        5000,
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(resp.vote_granted);
            assert!(resp.is_pre_vote);
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }

    // No state mutation.
    assert_eq!(n2_state.current_term, Term(5));
    assert_eq!(n2_state.voted_for, None);
}

#[test]
fn pre_vote_granted_when_leader_contact_expired() {
    // N2 heard from leader long ago.
    let mut n2_state = make_state(2);
    n2_state.current_term = Term(5);
    n2_state.last_leader_contact_ms = Some(500);

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(1),
        last_log_offset: Offset(10),
        last_log_term: Term(5),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut n2_state,
        &request,
        1000, // 500ms since last contact, > 150ms timeout
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(resp.vote_granted);
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Pre-Vote does not persist state
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_vote_request_does_not_update_term_or_voted_for() {
    let mut state = make_state(2);
    state.current_term = Term(3);
    state.voted_for = None;

    // Pre-vote with higher prospective term.
    let request = VoteRequest {
        term: Term(10),
        candidate_id: NodeId(1),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: true,
    };

    let _action = ElectionManager::handle_vote_request(
        &mut state,
        &request,
        5000,
        ELECTION_TIMEOUT_MS,
    );

    // No state mutation.
    assert_eq!(state.current_term, Term(3));
    assert_eq!(state.voted_for, None);
    assert_eq!(state.role, Role::Follower);
}

// ═══════════════════════════════════════════════════════════════════
// Log up-to-date check in pre-vote
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_vote_rejected_if_candidate_log_is_stale() {
    let mut state = make_state(2);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(20);
    state.last_leader_contact_ms = None;

    // Candidate has older log.
    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(3),
        last_log_offset: Offset(10),
        last_log_term: Term(4), // older term
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut state,
        &request,
        5000,
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(!resp.vote_granted, "should reject stale log");
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
}

#[test]
fn pre_vote_rejected_if_prospective_term_is_stale() {
    let mut state = make_state(2);
    state.current_term = Term(10);
    state.last_leader_contact_ms = None;

    let request = VoteRequest {
        term: Term(5), // stale
        candidate_id: NodeId(3),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut state,
        &request,
        5000,
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(!resp.vote_granted);
            assert_eq!(resp.term, Term(10));
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Response validation
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_vote_response_ignored_when_not_in_pre_vote_phase() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.election_phase = ElectionPhase::None;

    let resp = VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);

    assert_eq!(action, ElectionAction::None);
}

#[test]
fn real_vote_response_ignored_when_not_in_election_phase() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.election_phase = ElectionPhase::PreVote;

    let resp = VoteResponse { term: Term(5), vote_granted: false, is_pre_vote: false };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);

    assert_eq!(action, ElectionAction::None);
}

#[test]
fn duplicate_pre_vote_counted_once() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    let _action = ElectionManager::start_pre_vote(&mut state);

    // N2 grants twice.
    let resp = VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true };
    let a1 = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);
    assert_eq!(a1, ElectionAction::StartRealElection); // majority reached

    // Reset phase for testing duplicate.
    state.election_phase = ElectionPhase::PreVote;
    state.pre_votes_received.clear();
    state.pre_votes_received.insert(NodeId(1));

    // Same node grants again — should only count once.
    let _a2 = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);
    let _a3 = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);
    assert_eq!(state.pre_votes_received.len(), 2); // self + N2, not 3
}

#[test]
fn response_from_non_voter_ignored() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    let _action = ElectionManager::start_pre_vote(&mut state);

    // NodeId(99) is not in the voter set.
    let resp = VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(99), &resp);

    assert_eq!(action, ElectionAction::None);
    assert!(!state.pre_votes_received.contains(&NodeId(99)));
}

#[test]
fn response_with_higher_term_causes_step_down() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    let _action = ElectionManager::start_pre_vote(&mut state);

    let resp = VoteResponse { term: Term(10), vote_granted: false, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);

    assert_eq!(action, ElectionAction::PersistAndStepDown { new_term: Term(10) });
    assert_eq!(state.current_term, Term(10));
    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.election_phase, ElectionPhase::None);
}

// ═══════════════════════════════════════════════════════════════════
// Real vote handling (for completeness of the ElectionManager)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn real_vote_request_grants_and_persists() {
    let mut state = make_state(2);
    state.current_term = Term(5);

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(1),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: false,
    };

    let action = ElectionManager::handle_vote_request(
        &mut state,
        &request,
        5000,
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::PersistAndRespondVote { term, voted_for, response } => {
            assert_eq!(term, Term(6));
            assert_eq!(voted_for, Some(NodeId(1)));
            assert!(response.vote_granted);
            assert!(!response.is_pre_vote);
        }
        other => panic!("expected PersistAndRespondVote, got {other:?}"),
    }

    assert_eq!(state.current_term, Term(6));
    assert_eq!(state.voted_for, Some(NodeId(1)));
}

#[test]
fn real_vote_rejected_if_already_voted_for_other() {
    let mut state = make_state(2);
    state.current_term = Term(6);
    state.voted_for = Some(NodeId(3)); // already voted for N3

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(1),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: false,
    };

    let action = ElectionManager::handle_vote_request(
        &mut state,
        &request,
        5000,
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::PersistAndRespondVote { response, .. } => {
            assert!(!response.vote_granted);
        }
        other => panic!("expected PersistAndRespondVote, got {other:?}"),
    }
}

#[test]
fn real_election_becomes_leader_on_majority() {
    let mut state = make_state(1);
    state.current_term = Term(5);

    // Must go through pre-vote first.
    let _action = ElectionManager::start_pre_vote(&mut state);
    // N2 grants pre-vote → majority.
    let resp = VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true };
    let _action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);

    let action = ElectionManager::start_real_election(&mut state);
    assert_eq!(state.current_term, Term(6));
    assert_eq!(state.role, Role::Candidate);

    // Not a single-node cluster, so expect PersistAndSendVoteRequests.
    assert!(matches!(action, ElectionAction::PersistAndSendVoteRequests { .. }));

    // N2 grants.
    let resp = VoteResponse { term: Term(6), vote_granted: true, is_pre_vote: false };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);
    assert_eq!(action, ElectionAction::BecomeLeader);
}

// ═══════════════════════════════════════════════════════════════════
// Single-node cluster
// ═══════════════════════════════════════════════════════════════════

#[test]
fn single_node_pre_vote_immediately_starts_real_election() {
    let voters = vec![VoterInfo { node_id: NodeId(1), endpoint: "n1".into() }];
    let mut state = NodeState::new(NodeId(1), cluster_id(), voters);

    let action = ElectionManager::start_pre_vote(&mut state);
    assert_eq!(action, ElectionAction::StartRealElection);
}

#[test]
fn single_node_real_election_immediately_becomes_leader() {
    let voters = vec![VoterInfo { node_id: NodeId(1), endpoint: "n1".into() }];
    let mut state = NodeState::new(NodeId(1), cluster_id(), voters);

    // Single-node pre-vote returns StartRealElection immediately.
    let action = ElectionManager::start_pre_vote(&mut state);
    assert_eq!(action, ElectionAction::StartRealElection);

    // Now call start_real_election — pre-vote majority already reached (self).
    let action = ElectionManager::start_real_election(&mut state);
    assert_eq!(action, ElectionAction::BecomeLeader);
    assert_eq!(state.role, Role::Candidate); // event loop transitions to Leader
    assert_eq!(state.current_term, Term(1));
}

// ═══════════════════════════════════════════════════════════════════
// Pre-Vote gate enforcement
// ═══════════════════════════════════════════════════════════════════

#[test]
fn real_election_rejected_without_pre_vote() {
    // Calling start_real_election without a successful pre-vote returns None.
    let mut state = make_state(1);
    state.current_term = Term(5);

    let action = ElectionManager::start_real_election(&mut state);
    assert_eq!(action, ElectionAction::None);
    // Term must NOT be incremented.
    assert_eq!(state.current_term, Term(5));
    assert_eq!(state.voted_for, None);
}

#[test]
fn real_election_rejected_without_pre_vote_majority() {
    let mut state = make_state(1);
    state.current_term = Term(5);

    // Start pre-vote but don't get majority.
    let _action = ElectionManager::start_pre_vote(&mut state);
    assert_eq!(state.election_phase, ElectionPhase::PreVote);
    assert_eq!(state.pre_votes_received.len(), 1); // only self

    // Try real election — should be blocked.
    let action = ElectionManager::start_real_election(&mut state);
    assert_eq!(action, ElectionAction::None);
    assert_eq!(state.current_term, Term(5));
}

// ═══════════════════════════════════════════════════════════════════
// Stale pre-vote response rejection
// ═══════════════════════════════════════════════════════════════════

#[test]
fn stale_pre_vote_response_not_counted() {
    let mut state = make_state(1);
    state.current_term = Term(5);

    // Start pre-vote (prospective term = 6).
    let _action = ElectionManager::start_pre_vote(&mut state);

    // Response with term < current_term (stale) should be ignored.
    let stale_resp = VoteResponse { term: Term(3), vote_granted: true, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &stale_resp);
    assert_eq!(action, ElectionAction::None);
    assert_eq!(state.pre_votes_received.len(), 1); // only self
}

// ═══════════════════════════════════════════════════════════════════
// Leader validation in pre-vote rejection
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_vote_not_rejected_when_leader_contact_from_old_term() {
    // N2 heard from a leader but that leader was from an old term.
    let mut n2_state = make_state(2);
    n2_state.current_term = Term(5);
    n2_state.record_leader_contact(1000, NodeId(1), Term(3)); // old term

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(3),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut n2_state,
        &request,
        1050, // within timeout
        ELECTION_TIMEOUT_MS,
    );

    // Should grant because leader contact was from an old term.
    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(resp.vote_granted, "should grant: leader contact was from old term");
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
}

#[test]
fn pre_vote_not_rejected_when_no_leader_id() {
    // N2 has a last_leader_contact_ms but no leader_id (shouldn't happen
    // with the new API, but defensive).
    let mut n2_state = make_state(2);
    n2_state.current_term = Term(5);
    n2_state.last_leader_contact_ms = Some(1000);
    n2_state.leader_id = None; // no known leader
    n2_state.last_leader_term = None;

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(3),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut n2_state,
        &request,
        1050,
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(resp.vote_granted, "should grant: no valid leader_id");
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// PersistAndStepDown carries persistence requirement
// ═══════════════════════════════════════════════════════════════════

#[test]
fn step_down_returns_persist_action() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    let _action = ElectionManager::start_pre_vote(&mut state);

    let resp = VoteResponse { term: Term(10), vote_granted: false, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);

    // Must be PersistAndStepDown — event loop must persist before proceeding.
    match action {
        ElectionAction::PersistAndStepDown { new_term } => {
            assert_eq!(new_term, Term(10));
        }
        other => panic!("expected PersistAndStepDown, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Leader membership validation in pre-vote rejection
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_vote_not_rejected_when_leader_not_in_voter_set() {
    // N2 heard from NodeId(99) which is NOT in the 3-node voter set.
    // Even though the contact is recent and term matches, the leader is
    // not a valid cluster member, so the pre-vote should be granted.
    let mut n2_state = make_state(2);
    n2_state.current_term = Term(5);
    n2_state.last_leader_contact_ms = Some(1000);
    n2_state.leader_id = Some(NodeId(99)); // not in voter set
    n2_state.last_leader_term = Some(Term(5));

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(3),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut n2_state,
        &request,
        1050, // within timeout
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(resp.vote_granted, "should grant: leader not a valid voter/member");
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
}

#[test]
fn pre_vote_rejected_when_leader_is_valid_voter_member() {
    // N2 heard from N1 which IS in the voter set, at current term, recently.
    let mut n2_state = make_state(2);
    n2_state.current_term = Term(5);
    n2_state.record_leader_contact(1000, NodeId(1), Term(5));

    let request = VoteRequest {
        term: Term(6),
        candidate_id: NodeId(3),
        last_log_offset: Offset(0),
        last_log_term: Term(0),
        is_pre_vote: true,
    };

    let action = ElectionManager::handle_vote_request(
        &mut n2_state,
        &request,
        1050,
        ELECTION_TIMEOUT_MS,
    );

    match action {
        ElectionAction::RespondPreVote(resp) => {
            assert!(!resp.vote_granted, "should reject: valid leader member, current term, recent contact");
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Event-loop integration via on_election_timeout
// ═══════════════════════════════════════════════════════════════════

#[test]
fn on_election_timeout_starts_pre_vote_for_multi_node() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);

    let action = ElectionManager::on_election_timeout(&mut state);

    // Should send pre-vote requests (not jump to real election).
    assert_eq!(state.election_phase, ElectionPhase::PreVote);
    assert!(matches!(action, ElectionAction::SendPreVoteRequests(_)));
    // Term must NOT be incremented.
    assert_eq!(state.current_term, Term(5));
    assert_eq!(state.voted_for, None);
}

#[test]
fn on_election_timeout_single_node_becomes_leader() {
    let voters = vec![VoterInfo { node_id: NodeId(1), endpoint: "n1".into() }];
    let mut state = NodeState::new(NodeId(1), cluster_id(), voters);

    let action = ElectionManager::on_election_timeout(&mut state);

    // Single-node: pre-vote → real election → BecomeLeader, all chained.
    assert_eq!(action, ElectionAction::BecomeLeader);
    assert_eq!(state.current_term, Term(1));
    assert_eq!(state.voted_for, Some(NodeId(1)));
}

#[test]
fn on_election_timeout_then_responses_full_lifecycle() {
    // Full lifecycle: election_timeout → pre-vote → majority → real election → majority → leader
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);

    // Step 1: Election timeout fires → starts pre-vote.
    let action = ElectionManager::on_election_timeout(&mut state);
    assert!(matches!(action, ElectionAction::SendPreVoteRequests(_)));
    assert_eq!(state.current_term, Term(5)); // not incremented

    // Step 2: N2 grants pre-vote → StartRealElection.
    let resp = VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);
    assert_eq!(action, ElectionAction::StartRealElection);

    // Step 3: Event loop sees StartRealElection → calls proceed_to_real_election.
    let action = ElectionManager::proceed_to_real_election(&mut state);
    assert_eq!(state.current_term, Term(6)); // NOW incremented
    assert_eq!(state.voted_for, Some(NodeId(1)));
    assert_eq!(state.role, Role::Candidate);
    assert!(matches!(action, ElectionAction::PersistAndSendVoteRequests { .. }));

    // Step 4: N2 grants real vote → BecomeLeader.
    let resp = VoteResponse { term: Term(6), vote_granted: true, is_pre_vote: false };
    let action = ElectionManager::handle_vote_response(&mut state, NodeId(2), &resp);
    assert_eq!(action, ElectionAction::BecomeLeader);
}

// ═══════════════════════════════════════════════════════════════════
// Simulated cluster — partition scenario
// ═══════════════════════════════════════════════════════════════════

/// A minimal simulated cluster that holds per-node state and routes
/// messages through an explicit message queue. Partitions are modeled
/// by filtering which (src, dst) pairs can communicate.
mod sim_cluster {
    use super::*;
    use std::collections::{HashMap, VecDeque};

    /// Messages in flight between nodes.
    #[derive(Debug, Clone)]
    enum Message {
        PreVoteReq { from: NodeId, to: NodeId, req: VoteRequest },
        PreVoteResp { from: NodeId, to: NodeId, resp: VoteResponse },
        RealVoteReq { from: NodeId, to: NodeId, req: VoteRequest },
        RealVoteResp { from: NodeId, to: NodeId, resp: VoteResponse },
    }

    struct SimCluster {
        states: HashMap<NodeId, NodeState>,
        messages: VecDeque<Message>,
        /// Set of (src, dst) pairs that are partitioned (messages dropped).
        partitions: HashSet<(NodeId, NodeId)>,
        election_timeout_ms: u64,
        clock_ms: u64,
    }

    impl SimCluster {
        fn new(node_ids: &[u64]) -> Self {
            let voters: Vec<VoterInfo> = node_ids
                .iter()
                .map(|&id| VoterInfo {
                    node_id: NodeId(id),
                    endpoint: format!("n{id}"),
                })
                .collect();

            let mut states = HashMap::new();
            for &id in node_ids {
                states.insert(
                    NodeId(id),
                    NodeState::new(NodeId(id), cluster_id(), voters.clone()),
                );
            }

            SimCluster {
                states,
                messages: VecDeque::new(),
                partitions: HashSet::new(),
                election_timeout_ms: ELECTION_TIMEOUT_MS,
                clock_ms: 0,
            }
        }

        fn partition(&mut self, a: NodeId, b: NodeId) {
            self.partitions.insert((a, b));
            self.partitions.insert((b, a));
        }

        fn is_partitioned(&self, src: NodeId, dst: NodeId) -> bool {
            self.partitions.contains(&(src, dst))
        }

        fn record_leader_contact(&mut self, node: NodeId, leader: NodeId, term: Term) {
            let state = self.states.get_mut(&node).unwrap();
            state.record_leader_contact(self.clock_ms, leader, term);
        }

        fn advance_clock(&mut self, ms: u64) {
            self.clock_ms += ms;
        }

        /// Fire election timeout on `node_id`, enqueue resulting messages.
        fn fire_election_timeout(&mut self, node_id: NodeId) {
            let state = self.states.get_mut(&node_id).unwrap();
            let action = ElectionManager::on_election_timeout(state);
            self.enqueue_action(node_id, action);
        }

        fn enqueue_action(&mut self, src: NodeId, action: ElectionAction) {
            match action {
                ElectionAction::SendPreVoteRequests(targets) => {
                    for (dst, req) in targets {
                        self.messages.push_back(Message::PreVoteReq {
                            from: src,
                            to: dst,
                            req,
                        });
                    }
                }
                ElectionAction::PersistAndSendVoteRequests { requests, .. } => {
                    for (dst, req) in requests {
                        self.messages.push_back(Message::RealVoteReq {
                            from: src,
                            to: dst,
                            req,
                        });
                    }
                }
                _ => {} // other actions don't produce network messages
            }
        }

        /// Deliver all queued messages, respecting partitions. Returns
        /// aggregate actions produced (for inspection).
        fn deliver_all(&mut self) -> Vec<(NodeId, ElectionAction)> {
            let mut results = Vec::new();

            while let Some(msg) = self.messages.pop_front() {
                let (src, dst) = match &msg {
                    Message::PreVoteReq { from, to, .. } => (*from, *to),
                    Message::PreVoteResp { from, to, .. } => (*from, *to),
                    Message::RealVoteReq { from, to, .. } => (*from, *to),
                    Message::RealVoteResp { from, to, .. } => (*from, *to),
                };

                if self.is_partitioned(src, dst) {
                    continue; // message dropped
                }

                match msg {
                    Message::PreVoteReq { from, to, req } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let action = ElectionManager::handle_vote_request(
                            state,
                            &req,
                            self.clock_ms,
                            self.election_timeout_ms,
                        );
                        if let ElectionAction::RespondPreVote(resp) = action {
                            self.messages.push_back(Message::PreVoteResp {
                                from: to,
                                to: from,
                                resp,
                            });
                        }
                    }
                    Message::PreVoteResp { from, to, resp } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let action =
                            ElectionManager::handle_vote_response(state, from, &resp);
                        results.push((to, action.clone()));

                        // If StartRealElection, chain into real election.
                        if action == ElectionAction::StartRealElection {
                            let state = self.states.get_mut(&to).unwrap();
                            let next = ElectionManager::proceed_to_real_election(state);
                            results.push((to, next.clone()));
                            self.enqueue_action(to, next);
                        }
                    }
                    Message::RealVoteReq { from, to, req } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let action = ElectionManager::handle_vote_request(
                            state,
                            &req,
                            self.clock_ms,
                            self.election_timeout_ms,
                        );
                        if let ElectionAction::PersistAndRespondVote { response, .. } =
                            action
                        {
                            self.messages.push_back(Message::RealVoteResp {
                                from: to,
                                to: from,
                                resp: response,
                            });
                        }
                    }
                    Message::RealVoteResp { from, to, resp } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let action =
                            ElectionManager::handle_vote_response(state, from, &resp);
                        results.push((to, action));
                    }
                }
            }

            results
        }
    }

    // ── Scenario: Pre-Vote prevents disruptive election ──────────

    #[test]
    fn partition_scenario_n3_cannot_disrupt_via_pre_vote() {
        // Setup: 3-node cluster {N1, N2, N3}. N1 is leader at term 5.
        let mut cluster = SimCluster::new(&[1, 2, 3]);

        // Set all nodes to term 5.
        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
        }

        // N1 is leader; N2 and N3 know N1 is leader.
        cluster.states.get_mut(&NodeId(1)).unwrap().role = Role::Leader;
        cluster.states.get_mut(&NodeId(1)).unwrap().leader_id = Some(NodeId(1));

        // N2 heard from leader N1 recently (at t=0).
        cluster.record_leader_contact(NodeId(2), NodeId(1), Term(5));

        // N3 heard from leader earlier but is now partitioned.
        cluster.record_leader_contact(NodeId(3), NodeId(1), Term(5));

        // Partition N3 from N1 (but N3 can still reach N2).
        cluster.partition(NodeId(3), NodeId(1));

        // Time advances 50ms — within election timeout for N2.
        cluster.advance_clock(50);

        // N3's election timeout fires → starts pre-vote.
        cluster.fire_election_timeout(NodeId(3));

        // N3 should be in pre-vote phase.
        assert_eq!(
            cluster.states[&NodeId(3)].election_phase,
            ElectionPhase::PreVote
        );
        // N3's term must NOT have been incremented.
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(5));

        // Deliver messages: N3's pre-vote to N1 is dropped (partitioned),
        // N3's pre-vote to N2 is delivered. N2 rejects (recent leader contact).
        let results = cluster.deliver_all();

        // N3 should NOT have reached a majority for pre-vote.
        assert_eq!(cluster.states[&NodeId(3)].pre_votes_received.len(), 1); // only self

        // N3's term is still unchanged.
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(5));
        assert_eq!(cluster.states[&NodeId(3)].voted_for, None);
        assert_eq!(cluster.states[&NodeId(3)].role, Role::Follower);

        // No BecomeLeader or StartRealElection for N3.
        let n3_actions: Vec<_> = results
            .iter()
            .filter(|(id, _)| *id == NodeId(3))
            .collect();
        for (_, action) in &n3_actions {
            assert_ne!(*action, ElectionAction::StartRealElection);
            assert_ne!(*action, ElectionAction::BecomeLeader);
        }

        // N2's state is also unaffected.
        assert_eq!(cluster.states[&NodeId(2)].current_term, Term(5));
        assert_eq!(cluster.states[&NodeId(2)].voted_for, None);
    }

    // ── Scenario: Pre-Vote success when no leader exists ─────────

    #[test]
    fn no_leader_scenario_pre_vote_succeeds_and_wins_election() {
        // Setup: 3-node cluster, no leader, all at term 5.
        let mut cluster = SimCluster::new(&[1, 2, 3]);
        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
            state.last_log_term = Term(5);
            state.log_end_offset = Offset(10);
            // No leader contact — no one has heard from a leader.
            state.last_leader_contact_ms = None;
            state.leader_id = None;
        }

        // N1's election timeout fires.
        cluster.fire_election_timeout(NodeId(1));
        assert_eq!(
            cluster.states[&NodeId(1)].election_phase,
            ElectionPhase::PreVote
        );
        assert_eq!(cluster.states[&NodeId(1)].current_term, Term(5)); // not yet incremented

        // Deliver pre-vote requests to N2, N3. Both grant (no leader).
        // Responses come back to N1 → majority → StartRealElection → real election.
        let results = cluster.deliver_all();

        // After delivering all messages, N1 should have transitioned
        // to a real election (term incremented to 6).
        assert_eq!(cluster.states[&NodeId(1)].current_term, Term(6));
        assert_eq!(cluster.states[&NodeId(1)].role, Role::Candidate);
        assert_eq!(cluster.states[&NodeId(1)].election_phase, ElectionPhase::Election);
        assert_eq!(cluster.states[&NodeId(1)].voted_for, Some(NodeId(1)));

        // Real vote requests should have been queued and delivered.
        // Deliver any remaining messages (real vote responses).
        let results2 = cluster.deliver_all();

        // N1 should have won the election.
        let became_leader = results
            .iter()
            .chain(results2.iter())
            .any(|(id, action)| *id == NodeId(1) && *action == ElectionAction::BecomeLeader);
        assert!(became_leader, "N1 should have become leader");
    }

    // ── Scenario: Partition heals, then pre-vote succeeds ────────

    #[test]
    fn partition_heals_and_pre_vote_succeeds_after_timeout() {
        // N3 is partitioned, tries pre-vote, fails. Then N2's leader
        // contact expires, N3 retries, and succeeds.
        let mut cluster = SimCluster::new(&[1, 2, 3]);

        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
            state.last_log_term = Term(5);
            state.log_end_offset = Offset(10);
        }

        // N2 heard from leader N1 recently.
        cluster.record_leader_contact(NodeId(2), NodeId(1), Term(5));

        // Partition N3 from N1.
        cluster.partition(NodeId(3), NodeId(1));

        // N3's election timeout fires — pre-vote rejected by N2.
        cluster.advance_clock(50);
        cluster.fire_election_timeout(NodeId(3));
        let _results = cluster.deliver_all();

        // Confirm N3 did NOT start real election.
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(5));
        assert_ne!(cluster.states[&NodeId(3)].election_phase, ElectionPhase::Election);

        // Time passes beyond election timeout (>150ms from N2's last leader contact).
        cluster.advance_clock(200);

        // Reset N3's pre-vote state for a fresh attempt.
        {
            let n3 = cluster.states.get_mut(&NodeId(3)).unwrap();
            n3.election_phase = ElectionPhase::None;
            n3.pre_votes_received.clear();
            n3.pre_vote_term = None;
        }

        // N3 retries pre-vote.
        cluster.fire_election_timeout(NodeId(3));
        let results = cluster.deliver_all();

        // Now N2's leader contact has expired, so it grants the pre-vote.
        // N3 should proceed to real election.
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(6));
        assert_eq!(cluster.states[&NodeId(3)].role, Role::Candidate);

        // Deliver real vote responses.
        let results2 = cluster.deliver_all();

        let became_leader = results
            .iter()
            .chain(results2.iter())
            .any(|(id, action)| *id == NodeId(3) && *action == ElectionAction::BecomeLeader);
        // N3 can get N2's vote. N1 is partitioned, but N3+N2 = majority.
        assert!(became_leader, "N3 should become leader after partition heals timeout");
    }
}

// ═══════════════════════════════════════════════════════════════════
// ElectionDriver — event-loop integration tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn driver_election_timeout_starts_pre_vote() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);

    let batch = ElectionDriver::handle(&mut state, ElectionEvent::ElectionTimeout);

    assert_eq!(state.election_phase, ElectionPhase::PreVote);
    assert_eq!(state.current_term, Term(5)); // NOT incremented
    assert_eq!(state.voted_for, None);
    assert_eq!(batch.actions.len(), 1);
    assert!(matches!(batch.actions[0], ElectionAction::SendPreVoteRequests(_)));
}

#[test]
fn driver_single_node_timeout_becomes_leader() {
    let voters = vec![VoterInfo { node_id: NodeId(1), endpoint: "n1".into() }];
    let mut state = NodeState::new(NodeId(1), cluster_id(), voters);

    let batch = ElectionDriver::handle(&mut state, ElectionEvent::ElectionTimeout);

    // Single-node: pre-vote → real election → BecomeLeader, all chained.
    assert!(batch.actions.contains(&ElectionAction::BecomeLeader));
    assert_eq!(state.current_term, Term(1));
    assert_eq!(state.voted_for, Some(NodeId(1)));
}

#[test]
fn driver_full_lifecycle_timeout_to_leader() {
    let mut state = make_state(1);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);

    // Step 1: Election timeout → pre-vote requests.
    let batch = ElectionDriver::handle(&mut state, ElectionEvent::ElectionTimeout);
    assert_eq!(batch.actions.len(), 1);
    assert!(matches!(&batch.actions[0], ElectionAction::SendPreVoteRequests(_)));
    assert_eq!(state.current_term, Term(5));

    // Step 2: N2 grants pre-vote → driver automatically chains into real election.
    let batch = ElectionDriver::handle(
        &mut state,
        ElectionEvent::VoteResponseReceived {
            from: NodeId(2),
            response: VoteResponse { term: Term(5), vote_granted: true, is_pre_vote: true },
        },
    );
    // The batch should contain PersistAndSendVoteRequests (StartRealElection was resolved).
    assert!(
        batch.actions.iter().any(|a| matches!(a, ElectionAction::PersistAndSendVoteRequests { .. })),
        "expected PersistAndSendVoteRequests in batch, got: {:?}",
        batch.actions
    );
    assert_eq!(state.current_term, Term(6));
    assert_eq!(state.role, Role::Candidate);

    // Step 3: N2 grants real vote → BecomeLeader.
    let batch = ElectionDriver::handle(
        &mut state,
        ElectionEvent::VoteResponseReceived {
            from: NodeId(2),
            response: VoteResponse { term: Term(6), vote_granted: true, is_pre_vote: false },
        },
    );
    assert!(batch.actions.contains(&ElectionAction::BecomeLeader));
}

#[test]
fn driver_vote_request_pre_vote_rejected_with_leader_lease() {
    let mut state = make_state(2);
    state.current_term = Term(5);
    state.record_leader_contact(1000, NodeId(1), Term(5));

    let batch = ElectionDriver::handle(
        &mut state,
        ElectionEvent::VoteRequestReceived {
            request: VoteRequest {
                term: Term(6),
                candidate_id: NodeId(3),
                last_log_offset: Offset(10),
                last_log_term: Term(5),
                is_pre_vote: true,
            },
            now_ms: 1050,
            election_timeout_ms: ELECTION_TIMEOUT_MS,
        },
    );

    assert_eq!(batch.actions.len(), 1);
    match &batch.actions[0] {
        ElectionAction::RespondPreVote(resp) => {
            assert!(!resp.vote_granted);
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
    // No state mutation.
    assert_eq!(state.current_term, Term(5));
    assert_eq!(state.voted_for, None);
}

#[test]
fn driver_vote_request_pre_vote_granted_no_leader() {
    let mut state = make_state(2);
    state.current_term = Term(5);
    state.last_leader_contact_ms = None;

    let batch = ElectionDriver::handle(
        &mut state,
        ElectionEvent::VoteRequestReceived {
            request: VoteRequest {
                term: Term(6),
                candidate_id: NodeId(1),
                last_log_offset: Offset(0),
                last_log_term: Term(0),
                is_pre_vote: true,
            },
            now_ms: 5000,
            election_timeout_ms: ELECTION_TIMEOUT_MS,
        },
    );

    match &batch.actions[0] {
        ElectionAction::RespondPreVote(resp) => {
            assert!(resp.vote_granted);
        }
        other => panic!("expected RespondPreVote, got {other:?}"),
    }
    assert_eq!(state.current_term, Term(5));
    assert_eq!(state.voted_for, None);
}

// ═══════════════════════════════════════════════════════════════════
// ElectionDriver — simulated cluster with driver dispatch
// ═══════════════════════════════════════════════════════════════════

/// A simulated cluster that uses `ElectionDriver::handle` as the sole
/// entry point for election logic — exactly as a real event loop would.
mod driver_sim_cluster {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use xraft_core::election::ElectionActionBatch;

    #[derive(Debug, Clone)]
    enum Message {
        PreVoteReq { from: NodeId, to: NodeId, req: VoteRequest },
        PreVoteResp { from: NodeId, to: NodeId, resp: VoteResponse },
        RealVoteReq { from: NodeId, to: NodeId, req: VoteRequest },
        RealVoteResp { from: NodeId, to: NodeId, resp: VoteResponse },
    }

    struct DriverSimCluster {
        states: HashMap<NodeId, NodeState>,
        messages: VecDeque<Message>,
        partitions: HashSet<(NodeId, NodeId)>,
        election_timeout_ms: u64,
        clock_ms: u64,
    }

    impl DriverSimCluster {
        fn new(node_ids: &[u64]) -> Self {
            let voters: Vec<VoterInfo> = node_ids
                .iter()
                .map(|&id| VoterInfo {
                    node_id: NodeId(id),
                    endpoint: format!("n{id}"),
                })
                .collect();

            let mut states = HashMap::new();
            for &id in node_ids {
                states.insert(
                    NodeId(id),
                    NodeState::new(NodeId(id), cluster_id(), voters.clone()),
                );
            }

            DriverSimCluster {
                states,
                messages: VecDeque::new(),
                partitions: HashSet::new(),
                election_timeout_ms: ELECTION_TIMEOUT_MS,
                clock_ms: 0,
            }
        }

        fn partition(&mut self, a: NodeId, b: NodeId) {
            self.partitions.insert((a, b));
            self.partitions.insert((b, a));
        }

        fn is_partitioned(&self, src: NodeId, dst: NodeId) -> bool {
            self.partitions.contains(&(src, dst))
        }

        fn record_leader_contact(&mut self, node: NodeId, leader: NodeId, term: Term) {
            let state = self.states.get_mut(&node).unwrap();
            state.record_leader_contact(self.clock_ms, leader, term);
        }

        fn advance_clock(&mut self, ms: u64) {
            self.clock_ms += ms;
        }

        /// Fire election timeout via `ElectionDriver::handle`.
        fn fire_election_timeout(&mut self, node_id: NodeId) {
            let state = self.states.get_mut(&node_id).unwrap();
            let batch = ElectionDriver::handle(state, ElectionEvent::ElectionTimeout);
            self.enqueue_batch(node_id, &batch);
        }

        fn enqueue_batch(&mut self, src: NodeId, batch: &ElectionActionBatch) {
            for action in &batch.actions {
                self.enqueue_action(src, action);
            }
        }

        fn enqueue_action(&mut self, src: NodeId, action: &ElectionAction) {
            match action {
                ElectionAction::SendPreVoteRequests(targets) => {
                    for (dst, req) in targets {
                        self.messages.push_back(Message::PreVoteReq {
                            from: src,
                            to: *dst,
                            req: req.clone(),
                        });
                    }
                }
                ElectionAction::PersistAndSendVoteRequests { requests, .. } => {
                    for (dst, req) in requests {
                        self.messages.push_back(Message::RealVoteReq {
                            from: src,
                            to: *dst,
                            req: req.clone(),
                        });
                    }
                }
                _ => {}
            }
        }

        /// Deliver all queued messages using `ElectionDriver::handle`.
        fn deliver_all(&mut self) -> Vec<(NodeId, ElectionAction)> {
            let mut results = Vec::new();

            while let Some(msg) = self.messages.pop_front() {
                let (src, dst) = match &msg {
                    Message::PreVoteReq { from, to, .. }
                    | Message::PreVoteResp { from, to, .. }
                    | Message::RealVoteReq { from, to, .. }
                    | Message::RealVoteResp { from, to, .. } => (*from, *to),
                };

                if self.is_partitioned(src, dst) {
                    continue;
                }

                match msg {
                    Message::PreVoteReq { from, to, req } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let batch = ElectionDriver::handle(
                            state,
                            ElectionEvent::VoteRequestReceived {
                                request: req,
                                now_ms: self.clock_ms,
                                election_timeout_ms: self.election_timeout_ms,
                            },
                        );
                        for action in &batch.actions {
                            if let ElectionAction::RespondPreVote(resp) = action {
                                self.messages.push_back(Message::PreVoteResp {
                                    from: to,
                                    to: from,
                                    resp: resp.clone(),
                                });
                            }
                        }
                    }
                    Message::PreVoteResp { from, to, resp } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let batch = ElectionDriver::handle(
                            state,
                            ElectionEvent::VoteResponseReceived {
                                from,
                                response: resp,
                            },
                        );
                        for action in &batch.actions {
                            results.push((to, action.clone()));
                            self.enqueue_action(to, action);
                        }
                    }
                    Message::RealVoteReq { from, to, req } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let batch = ElectionDriver::handle(
                            state,
                            ElectionEvent::VoteRequestReceived {
                                request: req,
                                now_ms: self.clock_ms,
                                election_timeout_ms: self.election_timeout_ms,
                            },
                        );
                        for action in &batch.actions {
                            if let ElectionAction::PersistAndRespondVote { response, .. } = action {
                                self.messages.push_back(Message::RealVoteResp {
                                    from: to,
                                    to: from,
                                    resp: response.clone(),
                                });
                            }
                        }
                    }
                    Message::RealVoteResp { from, to, resp } => {
                        let state = self.states.get_mut(&to).unwrap();
                        let batch = ElectionDriver::handle(
                            state,
                            ElectionEvent::VoteResponseReceived {
                                from,
                                response: resp,
                            },
                        );
                        for action in &batch.actions {
                            results.push((to, action.clone()));
                        }
                    }
                }
            }

            results
        }
    }

    /// Scenario: Pre-Vote prevents disruptive election via driver dispatch.
    ///
    /// N3 is partitioned from leader N1. N3's election timeout fires, it
    /// sends pre-votes via ElectionDriver. N2 (which recently heard from N1)
    /// rejects the pre-vote through ElectionDriver. N3 cannot proceed.
    #[test]
    fn driver_partition_scenario_n3_cannot_disrupt() {
        let mut cluster = DriverSimCluster::new(&[1, 2, 3]);

        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
        }

        cluster.states.get_mut(&NodeId(1)).unwrap().role = Role::Leader;
        cluster.states.get_mut(&NodeId(1)).unwrap().leader_id = Some(NodeId(1));

        // N2 heard from leader N1 recently (at t=0).
        cluster.record_leader_contact(NodeId(2), NodeId(1), Term(5));
        // N3 heard from leader earlier.
        cluster.record_leader_contact(NodeId(3), NodeId(1), Term(5));

        // Partition N3 from N1.
        cluster.partition(NodeId(3), NodeId(1));

        // Time advances 50ms — within election timeout.
        cluster.advance_clock(50);

        // N3's election timeout fires via ElectionDriver.
        cluster.fire_election_timeout(NodeId(3));

        // N3 in pre-vote phase, term NOT incremented.
        assert_eq!(cluster.states[&NodeId(3)].election_phase, ElectionPhase::PreVote);
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(5));

        // Deliver messages via ElectionDriver.
        let results = cluster.deliver_all();

        // N3 did NOT get pre-vote majority.
        assert_eq!(cluster.states[&NodeId(3)].pre_votes_received.len(), 1);
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(5));
        assert_eq!(cluster.states[&NodeId(3)].voted_for, None);
        assert_eq!(cluster.states[&NodeId(3)].role, Role::Follower);

        // No StartRealElection or BecomeLeader for N3.
        for (id, action) in &results {
            if *id == NodeId(3) {
                assert_ne!(*action, ElectionAction::BecomeLeader);
            }
        }

        // N2 unaffected.
        assert_eq!(cluster.states[&NodeId(2)].current_term, Term(5));
        assert_eq!(cluster.states[&NodeId(2)].voted_for, None);
    }

    /// Scenario: Pre-Vote succeeds when no leader exists, via driver dispatch.
    ///
    /// No leader in the cluster. N1's election timeout fires via ElectionDriver.
    /// N2 and N3 grant pre-votes. ElectionDriver chains into real election.
    /// N2 and N3 grant real votes. N1 becomes leader.
    #[test]
    fn driver_no_leader_pre_vote_succeeds_and_wins() {
        let mut cluster = DriverSimCluster::new(&[1, 2, 3]);
        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
            state.last_log_term = Term(5);
            state.log_end_offset = Offset(10);
            state.last_leader_contact_ms = None;
            state.leader_id = None;
        }

        // N1's election timeout fires via ElectionDriver.
        cluster.fire_election_timeout(NodeId(1));
        assert_eq!(cluster.states[&NodeId(1)].election_phase, ElectionPhase::PreVote);
        assert_eq!(cluster.states[&NodeId(1)].current_term, Term(5));

        // Deliver pre-vote round (requests + responses, chained into real election).
        let results = cluster.deliver_all();

        // N1 should have transitioned to real election.
        assert_eq!(cluster.states[&NodeId(1)].current_term, Term(6));
        assert_eq!(cluster.states[&NodeId(1)].role, Role::Candidate);
        assert_eq!(cluster.states[&NodeId(1)].election_phase, ElectionPhase::Election);

        // Deliver real vote round.
        let results2 = cluster.deliver_all();

        let became_leader = results
            .iter()
            .chain(results2.iter())
            .any(|(id, action)| *id == NodeId(1) && *action == ElectionAction::BecomeLeader);
        assert!(became_leader, "N1 should have become leader via ElectionDriver");
    }

    /// Scenario: Partition heals after leader contact expires. N3 retries
    /// pre-vote via ElectionDriver and succeeds.
    #[test]
    fn driver_partition_heals_after_timeout() {
        let mut cluster = DriverSimCluster::new(&[1, 2, 3]);

        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
            state.last_log_term = Term(5);
            state.log_end_offset = Offset(10);
        }

        cluster.record_leader_contact(NodeId(2), NodeId(1), Term(5));
        cluster.partition(NodeId(3), NodeId(1));

        // First attempt: N3 pre-vote rejected.
        cluster.advance_clock(50);
        cluster.fire_election_timeout(NodeId(3));
        let _results = cluster.deliver_all();
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(5));

        // Time passes beyond election timeout.
        cluster.advance_clock(200);

        // Reset N3 for fresh attempt.
        {
            let n3 = cluster.states.get_mut(&NodeId(3)).unwrap();
            n3.election_phase = ElectionPhase::None;
            n3.pre_votes_received.clear();
            n3.pre_vote_term = None;
        }

        // Second attempt: N2's leader contact expired, pre-vote succeeds.
        cluster.fire_election_timeout(NodeId(3));
        let results = cluster.deliver_all();
        let results2 = cluster.deliver_all();

        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(6));
        let became_leader = results
            .iter()
            .chain(results2.iter())
            .any(|(id, action)| *id == NodeId(3) && *action == ElectionAction::BecomeLeader);
        assert!(became_leader, "N3 should become leader after timeout expires");
    }

    /// Edge case: leader from old term does not block pre-vote.
    /// N2 heard from a leader but at an old term — should NOT reject pre-vote.
    #[test]
    fn driver_old_term_leader_does_not_block_pre_vote() {
        let mut cluster = DriverSimCluster::new(&[1, 2, 3]);

        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
            state.last_log_term = Term(5);
            state.log_end_offset = Offset(10);
            state.last_leader_contact_ms = None;
        }

        // N2 heard from leader at OLD term 3 (not current term 5).
        {
            let n2 = cluster.states.get_mut(&NodeId(2)).unwrap();
            n2.last_leader_contact_ms = Some(0);
            n2.leader_id = Some(NodeId(1));
            n2.last_leader_term = Some(Term(3)); // old term
        }

        cluster.advance_clock(50); // within timeout

        // N3's election timeout fires.
        cluster.fire_election_timeout(NodeId(3));
        let _results = cluster.deliver_all();

        // N2 should have granted pre-vote (old-term leader doesn't count).
        // N3 should proceed to real election.
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(6));
        assert_eq!(cluster.states[&NodeId(3)].role, Role::Candidate);
    }

    /// Edge case: leader not in voter set does not block pre-vote.
    #[test]
    fn driver_non_member_leader_does_not_block_pre_vote() {
        let mut cluster = DriverSimCluster::new(&[1, 2, 3]);

        for state in cluster.states.values_mut() {
            state.current_term = Term(5);
            state.last_log_term = Term(5);
            state.log_end_offset = Offset(10);
            state.last_leader_contact_ms = None;
        }

        // N2 heard from NodeId(99) — NOT a cluster member.
        {
            let n2 = cluster.states.get_mut(&NodeId(2)).unwrap();
            n2.last_leader_contact_ms = Some(0);
            n2.leader_id = Some(NodeId(99)); // not in voter set
            n2.last_leader_term = Some(Term(5));
        }

        cluster.advance_clock(50);

        cluster.fire_election_timeout(NodeId(3));
        let _results = cluster.deliver_all();

        // N2 should grant: leader is not a valid cluster member.
        assert_eq!(cluster.states[&NodeId(3)].current_term, Term(6));
        assert_eq!(cluster.states[&NodeId(3)].role, Role::Candidate);
    }
}

// ═══════════════════════════════════════════════════════════════════
// ElectionExecutor: pre-vote persistence guarantees
// ═══════════════════════════════════════════════════════════════════

#[test]
fn executor_pre_vote_does_not_persist() {
    let mut state = make_state(1);
    state.current_term = Term(3);
    let mut store = InMemoryQuorumStateStore::new();

    let (batch, result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::ElectionTimeout,
        &mut store,
    );

    // Pre-vote phase: no persistence should have occurred.
    assert_eq!(store.persist_count, 0);
    assert!(!result.pre_vote_requests.is_empty());
    assert!(result.vote_requests.is_empty());
    assert!(!result.became_leader);
    assert!(result.persist_failed.is_none());

    // Term must remain unchanged.
    assert_eq!(state.current_term, Term(3));
    assert_eq!(state.voted_for, None);
    assert_eq!(state.election_phase, ElectionPhase::PreVote);

    // The batch should contain SendPreVoteRequests, not any persist action.
    assert_eq!(batch.actions.len(), 1);
    assert!(matches!(
        &batch.actions[0],
        ElectionAction::SendPreVoteRequests(_)
    ));
}

#[test]
fn executor_pre_vote_response_handling_does_not_persist() {
    let mut state = make_state(1);
    state.current_term = Term(3);
    let mut store = InMemoryQuorumStateStore::new();

    // Start pre-vote.
    let _ = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::ElectionTimeout,
        &mut store,
    );
    assert_eq!(store.persist_count, 0);

    // Receive a pre-vote grant from N2 — should NOT persist.
    let (_batch, result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::VoteResponseReceived {
            from: NodeId(2),
            response: VoteResponse {
                term: Term(3),
                vote_granted: true,
                is_pre_vote: true,
            },
        },
        &mut store,
    );

    // Pre-vote majority reached → transitions to real election which DOES persist.
    // But the pre-vote grant itself didn't persist — only the real election does.
    assert!(store.persist_count > 0, "real election persists term+vote");
    assert_eq!(state.current_term, Term(4), "real election increments term");
    assert_eq!(state.voted_for, Some(NodeId(1)));
    assert_eq!(state.election_phase, ElectionPhase::Election);

    // The real election sends vote requests (persisted).
    assert!(!result.vote_requests.is_empty());
}

#[test]
fn executor_responding_to_pre_vote_does_not_persist() {
    let mut state = make_state(2);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);
    let mut store = InMemoryQuorumStateStore::new();

    // Receive a pre-vote request from N1.
    let (_batch, result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::VoteRequestReceived {
            request: VoteRequest {
                term: Term(6),
                candidate_id: NodeId(1),
                last_log_offset: Offset(10),
                last_log_term: Term(5),
                is_pre_vote: true,
            },
            now_ms: 1000,
            election_timeout_ms: ELECTION_TIMEOUT_MS,
        },
        &mut store,
    );

    // No persistence for pre-vote responses.
    assert_eq!(store.persist_count, 0);
    assert!(result.pre_vote_response.is_some());
    assert!(result.pre_vote_response.as_ref().unwrap().vote_granted);
    assert!(result.vote_response.is_none());

    // Node state unchanged.
    assert_eq!(state.current_term, Term(5));
    assert_eq!(state.voted_for, None);
}

#[test]
fn executor_full_lifecycle_pre_vote_to_leader() {
    let mut state = make_state(1);
    state.current_term = Term(3);
    state.last_log_term = Term(3);
    state.log_end_offset = Offset(10);
    let mut store = InMemoryQuorumStateStore::new();

    // Step 1: Election timeout → pre-vote (no persist).
    let _ = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::ElectionTimeout,
        &mut store,
    );
    assert_eq!(store.persist_count, 0);
    assert_eq!(state.election_phase, ElectionPhase::PreVote);

    // Step 2: Pre-vote grant from N2 → real election (persists).
    let (_batch, result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::VoteResponseReceived {
            from: NodeId(2),
            response: VoteResponse {
                term: Term(3),
                vote_granted: true,
                is_pre_vote: true,
            },
        },
        &mut store,
    );
    assert_eq!(store.persist_count, 1, "real election persists once");
    assert_eq!(state.current_term, Term(4));
    assert_eq!(state.election_phase, ElectionPhase::Election);
    assert!(!result.became_leader);

    // Step 3: Real vote grant from N2 → become leader (persists again).
    let (_batch, result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::VoteResponseReceived {
            from: NodeId(2),
            response: VoteResponse {
                term: Term(4),
                vote_granted: true,
                is_pre_vote: false,
            },
        },
        &mut store,
    );
    assert_eq!(store.persist_count, 2, "leader transition persists");
    assert!(result.became_leader);

    // Verify final state: leader at term 4, voted for self.
    assert_eq!(state.current_term, Term(4));
    assert_eq!(state.voted_for, Some(NodeId(1)));
}

#[test]
fn executor_pre_vote_rejected_by_leader_lease_no_persist() {
    // N2 recently heard from leader N1 — should reject N3's pre-vote.
    let mut state = make_state(2);
    state.current_term = Term(5);
    state.last_log_term = Term(5);
    state.log_end_offset = Offset(10);
    state.record_leader_contact(100, NodeId(1), Term(5));
    let mut store = InMemoryQuorumStateStore::new();

    let (_batch, result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::VoteRequestReceived {
            request: VoteRequest {
                term: Term(6),
                candidate_id: NodeId(3),
                last_log_offset: Offset(10),
                last_log_term: Term(5),
                is_pre_vote: true,
            },
            now_ms: 120, // within election_timeout_ms of last contact
            election_timeout_ms: ELECTION_TIMEOUT_MS,
        },
        &mut store,
    );

    assert_eq!(store.persist_count, 0);
    assert!(result.pre_vote_response.is_some());
    assert!(!result.pre_vote_response.as_ref().unwrap().vote_granted);

    // State unchanged.
    assert_eq!(state.current_term, Term(5));
    assert_eq!(state.voted_for, None);
}

#[test]
fn executor_fetch_response_records_leader_contact() {
    let mut state = make_state(2);
    state.current_term = Term(5);
    let mut store = InMemoryQuorumStateStore::new();

    let (_batch, _result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::FetchResponseReceived {
            response: FetchResponse {
                term: Term(5),
                leader_id: NodeId(1),
                high_watermark: Offset(10),
            },
            now_ms: 500,
        },
        &mut store,
    );

    // FetchResponse at same term records leader contact but doesn't persist.
    assert_eq!(store.persist_count, 0);
    assert_eq!(state.last_leader_contact_ms, Some(500));
    assert_eq!(state.leader_id, Some(NodeId(1)));
    assert_eq!(state.last_leader_term, Some(Term(5)));
}

#[test]
fn executor_persist_failure_rolls_back_state() {
    let mut state = make_state(1);
    state.current_term = Term(3);
    state.last_log_term = Term(3);
    state.log_end_offset = Offset(10);
    let mut store = InMemoryQuorumStateStore::new();

    // Start pre-vote (no persist needed, always succeeds).
    let _ = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::ElectionTimeout,
        &mut store,
    );

    // Inject a persistence failure for the real election.
    store.fail_next_persist = Some("disk full".into());

    // Pre-vote grant → real election → persist fails → rollback.
    let (_batch, result) = ElectionExecutor::handle_event(
        &mut state,
        ElectionEvent::VoteResponseReceived {
            from: NodeId(2),
            response: VoteResponse {
                term: Term(3),
                vote_granted: true,
                is_pre_vote: true,
            },
        },
        &mut store,
    );

    assert!(result.persist_failed.is_some());
    assert_eq!(result.persist_failed.as_deref(), Some("disk full"));

    // State should be rolled back to pre-event snapshot.
    // The pre-vote grant was recorded but real election was rolled back.
    assert_eq!(state.current_term, Term(3), "term rolled back");
}
