use std::net::SocketAddr;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::log_entry::LogEntry;
use crate::types::{NodeId, Term};
use crate::voter::Endpoint;

/// RPC request to add a new voter to the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddVoterRequest {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
}

/// RPC request to remove a voter from the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveVoterRequest {
    pub node_id: NodeId,
}

/// RPC request to update a voter's endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateVoterRequest {
    pub node_id: NodeId,
    pub new_endpoint: Endpoint,
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
