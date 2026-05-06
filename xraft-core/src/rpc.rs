use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::log_entry::LogEntry;
use crate::types::{ClusterId, NodeId, Term};

/// Wire envelope carrying identity, fencing, and payload fields.
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

// --- Vote (Election) ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_offset: u64,
    pub last_log_term: Term,
    pub is_pre_vote: bool,
}

/// RPC request to register a new observer (non-voting node).
/// Must be sent to the leader; non-leaders reject with `NotLeader`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterObserverRequest {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
}

/// RPC response to observer registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterObserverResponse {
    pub success: bool,
    pub leader_id: Option<NodeId>,
    pub error: Option<MembershipError>,
}

impl RegisterObserverResponse {
    pub fn success(leader_id: Option<NodeId>) -> Self {
        Self {
            success: true,
            leader_id,
            error: None,
        }
    }

    pub fn error(error: MembershipError, leader_id: Option<NodeId>) -> Self {
        Self {
            success: false,
            leader_id,
            error: Some(error),
        }
    }
}

/// RPC request to deregister an observer (non-voting node).
/// Must be sent to the leader; non-leaders reject with `NotLeader`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeregisterObserverRequest {
    pub node_id: NodeId,
}

/// RPC response to observer deregistration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeregisterObserverResponse {
    pub success: bool,
    pub leader_id: Option<NodeId>,
    pub error: Option<MembershipError>,
}

impl DeregisterObserverResponse {
    pub fn success(leader_id: Option<NodeId>) -> Self {
        Self {
            success: true,
            leader_id,
            error: None,
        }
    }

    pub fn error(error: MembershipError, leader_id: Option<NodeId>) -> Self {
        Self {
            success: false,
            leader_id,
            error: Some(error),
        }
    }
}

/// Fetch RPC request sent by followers and observers to the leader
/// (architecture §3.3 `Fetch`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    /// Follower/observer sending the request.
    pub replica_id: NodeId,
    /// Next offset the follower wants to read (= follower's log_end_offset).
    pub fetch_offset: u64,
    /// Epoch of the follower's last log entry.
    pub last_fetched_epoch: Term,
    /// Maximum response size in bytes (0 = unlimited up to server default).
    pub max_bytes: u32,
}

/// Divergence indicator returned in FetchResponse when the follower's
/// log has diverged from the leader's (architecture §3.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergingEpoch {
    /// The epoch where divergence was detected.
    pub epoch: Term,
    /// The offset the follower should truncate to.
    pub end_offset: u64,
}

/// Snapshot identifier returned in FetchResponse when the follower
/// needs a snapshot transfer (fetch_offset < log_start_offset).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotId {
    /// Last offset included in the snapshot.
    pub end_offset: u64,
    /// Term of the last entry in the snapshot.
    pub epoch: Term,
}

/// Fetch RPC response from the leader to a follower/observer
/// (architecture §3.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub leader_id: NodeId,
    pub leader_epoch: Term,
    /// Exclusive upper bound: entries with offset < HW are committed.
    pub high_watermark: u64,
    /// Leader's log start (after compaction).
    pub log_start_offset: u64,
    /// Log entries starting at the requested fetch_offset.
    pub entries: Vec<LogEntry>,
    /// Set when log divergence is detected — follower must truncate.
    pub diverging_epoch: Option<DivergingEpoch>,
    /// Set when fetch_offset < log_start_offset — follower needs snapshot.
    pub snapshot_id: Option<SnapshotId>,
}

/// Response to any membership change RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipChangeResponse {
    pub success: bool,
    /// If the recipient is not the leader, this field indicates the known
    /// leader so the client can redirect.
    pub leader_id: Option<NodeId>,
    pub error: Option<MembershipError>,
}

/// Error variants for membership change operations (architecture §3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipError {
    /// This node is not the leader; `leader_id` hints at who is.
    NotLeader { leader_id: Option<NodeId> },
    /// An uncommitted VotersRecord already exists in the log.
    ChangeInProgress,
    /// The target node is already a voter.
    NodeAlreadyVoter,
    /// The target node is not known (not registered as an observer).
    NodeNotFound,
    /// The observer's fetch_offset has not reached within threshold of
    /// the leader's log end.
    NodeNotCaughtUp,
}

impl MembershipChangeResponse {
    pub fn success(leader_id: Option<NodeId>) -> Self {
        MembershipChangeResponse {
            success: true,
            leader_id,
            error: None,
        }
    }

    pub fn error(error: MembershipError, leader_id: Option<NodeId>) -> Self {
        MembershipChangeResponse {
            success: false,
            leader_id,
            error: Some(error),
        }
    }
}

// --- Fetch (Log Replication) ---

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergingEpoch {
    pub epoch: Term,
    pub end_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: Term,
}

// --- FetchSnapshot (Snapshot Transfer) ---

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
    pub data: Bytes,
    pub is_last_chunk: bool,
}

// --- Membership Change RPCs ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddVoterRequest {
    pub node_id: NodeId,
    pub endpoint: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveVoterRequest {
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateVoterRequest {
    pub node_id: NodeId,
    pub new_endpoint: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipChangeResponse {
    pub success: bool,
    pub leader_id: Option<NodeId>,
    pub error: Option<MembershipError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipError {
    NotLeader { leader_id: Option<NodeId> },
    ChangeInProgress,
    NodeAlreadyVoter,
    NodeNotFound,
    NodeNotCaughtUp,
}
