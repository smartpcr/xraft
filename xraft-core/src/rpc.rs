use crate::log_entry::LogEntry;
use crate::types::{ClusterId, NodeId, Term};
use serde::{Deserialize, Serialize};

use crate::log_entry::LogEntry;
use crate::snapshot::SnapshotId;
use crate::types::{ClusterId, NodeId, Term};
use crate::voter::{VoterInfo, VotersRecord};

/// Envelope wrapping every RPC message with identity and fencing fields.
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_offset: Offset,
    pub last_log_term: Term,
    pub is_pre_vote: bool,
}

/// Election response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
    pub is_pre_vote: bool,
}

/// Pull-based log replication request from follower to leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRequest {
    /// Follower/observer sending the request.
    pub replica_id: NodeId,
    /// Next offset the follower wants to read (= follower's log_end_offset).
    pub fetch_offset: u64,
    /// Epoch of the follower's last log entry.
    pub last_fetched_epoch: Term,
    /// Maximum response payload size.
    pub max_bytes: u32,
}

/// Leader's response to a Fetch request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchResponse {
    pub term: Term,
    pub leader_id: NodeId,
    pub leader_epoch: Term,
    /// Exclusive upper bound: entries with offset < HW are committed.
    pub high_watermark: u64,
    /// Leader's log start (after compaction).
    pub log_start_offset: u64,
    /// Log entries starting at the requested fetch_offset.
    pub entries: Vec<LogEntry>,
    /// Set when log divergence is detected.
    pub diverging_epoch: Option<DivergingEpoch>,
    /// Set when fetch_offset < log_start_offset (need snapshot).
    pub snapshot_id: Option<SnapshotId>,
}

/// Instructs follower to truncate to resolve divergence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergingEpoch {
    /// The epoch where divergence was found.
    pub epoch: Term,
    /// The offset to truncate to.
    pub end_offset: u64,
}

/// Identifies a snapshot for transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotId {
    /// Last offset included in snapshot.
    pub end_offset: u64,
    /// Term of last entry in snapshot.
    pub epoch: Term,
}

/// Snapshot transfer request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchSnapshotRequest {
    pub snapshot_id: SnapshotId,
    pub position: u64,
    pub max_bytes: u32,
}

/// Snapshot transfer response chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchSnapshotResponse {
    pub snapshot_id: SnapshotId,
    pub position: u64,
    pub data: bytes::Bytes,
    pub is_last_chunk: bool,
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
    pub entries: Vec<LogEntry>,
    pub diverging_epoch: Option<DivergingEpoch>,
    pub snapshot_id: Option<SnapshotId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergingEpoch {
    pub epoch: Term,
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchSnapshotRequest {
    pub snapshot_id: SnapshotId,
    pub position: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchSnapshotResponse {
    pub snapshot_id: SnapshotId,
    pub position: u64,
    pub data: bytes::Bytes,
    pub is_last_chunk: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddVoterRequest {
    pub node_id: NodeId,
    pub endpoint: VoterInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveVoterRequest {
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateVoterRequest {
    pub voter: VoterInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipChangeResponse {
    pub result: std::result::Result<VotersRecord, MembershipError>,
}

/// Errors returned by membership-change RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipError {
    NotLeader,
    ChangeInProgress,
    NodeAlreadyVoter,
    NodeNotFound,
    NodeNotCaughtUp,
}
