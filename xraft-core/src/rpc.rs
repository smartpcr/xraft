use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{ClusterId, NodeId, Term};
use crate::voter::VoterInfo;

/// Identity wrapper for every RPC message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub cluster_id: ClusterId,
    pub leader_epoch: Term,
    pub source: NodeId,
    pub payload: RpcPayload,
}

/// Discriminated union of all RPC messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcPayload {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    FetchSnapshotRequest(FetchSnapshotRequest),
    FetchSnapshotResponse(FetchSnapshotResponse),
    AddVoterRequest(AddVoterRequest),
    RemoveVoterRequest(RemoveVoterRequest),
    UpdateVoterRequest(UpdateVoterRequest),
    MembershipChangeResponse(MembershipChangeResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_offset: u64,
    pub last_log_term: Term,
    pub is_pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
    pub is_pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub replica_id: NodeId,
    pub fetch_offset: u64,
    pub last_fetched_epoch: Term,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub leader_id: NodeId,
    pub leader_epoch: Term,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    pub entries: Vec<crate::log_entry::LogEntry>,
    pub diverging_epoch: Option<DivergingEpoch>,
    pub snapshot_id: Option<SnapshotId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergingEpoch {
    pub epoch: Term,
    pub end_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSnapshotRequest {
    pub replica_id: NodeId,
    pub snapshot_id: SnapshotId,
    pub position: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSnapshotResponse {
    pub data: Bytes,
    pub is_last_chunk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddVoterRequest {
    pub voter: VoterInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveVoterRequest {
    pub node_id: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateVoterRequest {
    pub voter: VoterInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipChangeResponse {
    pub result: Result<(), MembershipError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipError {
    NotLeader,
    ChangeInProgress,
    NodeAlreadyVoter,
    NodeNotFound,
    NodeNotCaughtUp,
}
