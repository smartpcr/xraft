use std::net::SocketAddr;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{ClusterId, NodeId, Term};

/// Wrapper envelope for all RPC messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub cluster_id: ClusterId,
    pub leader_epoch: u64,
    pub source: NodeId,
    pub payload: RpcPayload,
}

/// Discriminated union of all RPC message types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcPayload {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
}

/// Request to update a voter's endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateVoterRequest {
    pub node_id: NodeId,
    pub new_endpoint: SocketAddr,
}

/// Response to any membership-change RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipChangeResponse {
    pub success: bool,
    /// If the receiving node is not the leader, redirects to the known leader.
    pub leader_id: Option<NodeId>,
    pub error: Option<MembershipError>,
}

/// Reasons a membership-change request can be rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipError {
    /// The receiving node is not the current leader.
    NotLeader { leader_id: Option<NodeId> },
    /// An uncommitted VotersRecord already exists in the log.
    ChangeInProgress,
    /// The node is already a voter in the current configuration.
    NodeAlreadyVoter,
    /// The node was not found in the current configuration.
    NodeNotFound,
    /// The observer's fetch_offset is behind the leader's current HW.
    NodeNotCaughtUp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRequest {
    pub replica_id: NodeId,
    pub fetch_offset: u64,
    pub last_fetched_epoch: Term,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchResponse {
    pub leader_id: NodeId,
    pub leader_epoch: Term,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    pub entries: Vec<crate::log_entry::LogEntry>,
    pub diverging_epoch: Option<DivergingEpoch>,
    pub snapshot_id: Option<SnapshotId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergingEpoch {
    pub epoch: Term,
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: Term,
}
