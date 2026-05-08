use std::net::SocketAddr;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::log_entry::LogEntry;
use crate::types::{ClusterId, NodeId, Offset, Term};

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

/// Every RPC is wrapped in an envelope carrying cluster identity, fencing
/// epoch, and source node for identity verification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub cluster_id: ClusterId,
    pub leader_epoch: Term,
    pub source: NodeId,
    pub payload: RpcPayload,
}

/// Discriminated union of all xraft RPC message types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

// ---------------------------------------------------------------------------
// Vote (Election)
// ---------------------------------------------------------------------------

/// Sent by a candidate to request votes from peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_offset: Offset,
    pub last_log_term: Term,
    /// `true` for the Pre-Vote phase (does not increment term).
    pub is_pre_vote: bool,
}

/// Response to a `VoteRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
    pub is_pre_vote: bool,
}

// ---------------------------------------------------------------------------
// Fetch (Log Replication)
// ---------------------------------------------------------------------------

/// Sent by a follower/observer to pull log entries from the leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRequest {
    pub replica_id: NodeId,
    pub fetch_offset: Offset,
    pub last_fetched_epoch: Term,
    pub max_bytes: u32,
}

/// Leader's response carrying log entries and metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchResponse {
    pub leader_id: NodeId,
    pub leader_epoch: Term,
    /// Exclusive upper bound: entries with offset < HW are committed.
    pub high_watermark: Offset,
    /// Leader's log start offset (after compaction).
    pub log_start_offset: Offset,
    pub entries: Vec<LogEntry>,
    /// Set when log divergence is detected.
    pub diverging_epoch: Option<DivergingEpoch>,
    /// Set when `fetch_offset < log_start_offset` (follower needs snapshot).
    pub snapshot_id: Option<SnapshotId>,
}

// ---------------------------------------------------------------------------
// Helper structs
// ---------------------------------------------------------------------------

/// Indicates where the follower's log diverges from the leader's.
/// The follower should truncate its log to `end_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergingEpoch {
    pub epoch: Term,
    pub end_offset: Offset,
}

/// Identifies a specific snapshot by its last included offset and epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: Offset,
    pub epoch: Term,
}

// ---------------------------------------------------------------------------
// FetchSnapshot (Snapshot Transfer)
// ---------------------------------------------------------------------------

/// Sent by a follower to download a snapshot from the leader in chunks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchSnapshotRequest {
    pub snapshot_id: SnapshotId,
    /// Byte offset into the snapshot file.
    pub position: u64,
    pub max_bytes: u32,
}

/// A chunk of a snapshot being transferred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchSnapshotResponse {
    pub snapshot_id: SnapshotId,
    /// Byte offset of this chunk within the snapshot.
    pub position: u64,
    pub data: Bytes,
    pub is_last_chunk: bool,
}

// ---------------------------------------------------------------------------
// Membership Change RPCs
// ---------------------------------------------------------------------------

/// Request to add a new voter to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddVoterRequest {
    pub node_id: NodeId,
    pub endpoint: SocketAddr,
}

/// Request to remove an existing voter from the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveVoterRequest {
    pub node_id: NodeId,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn rpc_envelope_vote_request_roundtrip() {
        let envelope = RpcEnvelope {
            cluster_id: ClusterId(Uuid::from_u128(0xdead_beef_cafe_babe_1234_5678_9abc_def0)),
            leader_epoch: Term(5),
            source: NodeId(42),
            payload: RpcPayload::VoteRequest(VoteRequest {
                term: Term(6),
                candidate_id: NodeId(42),
                last_log_offset: Offset(100),
                last_log_term: Term(4),
                is_pre_vote: true,
            }),
        };

        let encoded = bincode::serialize(&envelope).expect("serialize");
        let decoded: RpcEnvelope = bincode::deserialize(&encoded).expect("deserialize");

        assert_eq!(envelope, decoded);
    }

    #[test]
    fn rpc_envelope_fetch_snapshot_response_roundtrip() {
        let envelope = RpcEnvelope {
            cluster_id: ClusterId(Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888)),
            leader_epoch: Term(10),
            source: NodeId(1),
            payload: RpcPayload::FetchSnapshotResponse(FetchSnapshotResponse {
                snapshot_id: SnapshotId {
                    end_offset: Offset(500),
                    epoch: Term(9),
                },
                position: 4096,
                data: Bytes::from_static(b"snapshot-chunk-data"),
                is_last_chunk: false,
            }),
        };

        let encoded = bincode::serialize(&envelope).expect("serialize");
        let decoded: RpcEnvelope = bincode::deserialize(&encoded).expect("deserialize");

        assert_eq!(envelope, decoded);
    }

    #[test]
    fn membership_error_exhaustive_match() {
        let variants: Vec<MembershipError> = vec![
            MembershipError::NotLeader {
                leader_id: Some(NodeId(1)),
            },
            MembershipError::ChangeInProgress,
            MembershipError::NodeAlreadyVoter,
            MembershipError::NodeNotFound,
            MembershipError::NodeNotCaughtUp,
        ];

        for error in &variants {
            // Exhaustive match — if a variant is added without updating this
            // test, compilation will fail.
            match error {
                MembershipError::NotLeader { leader_id } => {
                    assert!(leader_id.is_some());
                }
                MembershipError::ChangeInProgress => {}
                MembershipError::NodeAlreadyVoter => {}
                MembershipError::NodeNotFound => {}
                MembershipError::NodeNotCaughtUp => {}
            }
        }

        assert_eq!(variants.len(), 5, "all five MembershipError variants must be covered");
    }
}
