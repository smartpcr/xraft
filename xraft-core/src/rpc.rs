use std::net::SocketAddr;

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

/// Request to update the endpoint of an existing voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateVoterRequest {
    pub node_id: NodeId,
    pub new_endpoint: SocketAddr,
}

/// Response to a membership change request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipChangeResponse {
    pub success: bool,
    pub error: Option<MembershipError>,
}

impl MembershipChangeResponse {
    pub fn ok() -> Self {
        Self {
            success: true,
            error: None,
        }
    }

    pub fn err(error: MembershipError) -> Self {
        Self {
            success: false,
            error: Some(error),
        }
    }
}

/// Errors that can occur during membership changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipError {
    /// This node is not the leader; includes leader_id for redirection.
    NotLeader { leader_id: Option<NodeId> },
    /// Another membership change is already in progress.
    ChangeInProgress,
    /// The node is already a voter.
    NodeAlreadyVoter,
    /// The node was not found in the voter set.
    NodeNotFound,
    /// The observer has not caught up to the leader's log.
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
