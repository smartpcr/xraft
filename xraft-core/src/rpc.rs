use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Offset, Term};

/// Request sent by a candidate to request a vote (or pre-vote) from a peer.
///
/// When `is_pre_vote` is `true`, the `term` field carries the *prospective*
/// next term (`current_term + 1`). The candidate has NOT yet incremented its
/// own term or persisted any state — this is a speculative viability check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_offset: Offset,
    pub last_log_term: Term,
    pub is_pre_vote: bool,
}

/// Response to a `VoteRequest`.
///
/// For pre-vote responses, the voter does NOT update its own term or
/// `voted_for` — no durable state is mutated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
    pub is_pre_vote: bool,
}

/// Response from a leader to a follower's Fetch RPC.
///
/// Followers use the `leader_id` and `term` fields to track the last
/// valid leader contact, which gates pre-vote rejection (the leader
/// lease check). This is the primary mechanism through which followers
/// learn that a leader is alive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchResponse {
    pub term: Term,
    pub leader_id: NodeId,
    pub high_watermark: Offset,
}
