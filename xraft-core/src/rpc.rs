use serde::{Deserialize, Serialize};

use crate::types::{ClusterId, NodeId, Term};
use crate::voter::{VoterInfo, VotersRecord};

/// Envelope wrapping every RPC message with cluster and leader identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub cluster_id: ClusterId,
    pub leader_epoch: Term,
    pub source: NodeId,
    pub payload: RpcPayload,
}

/// Discriminated union of all RPC message types.
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
    pub snapshot_id: SnapshotId,
    pub position: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchSnapshotResponse {
    pub data: bytes::Bytes,
    pub position: u64,
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
    pub success: bool,
    pub error: Option<MembershipError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipError {
    NotLeader { leader_id: Option<NodeId> },
    ChangeInProgress,
    NodeAlreadyVoter,
    NodeNotFound,
    NodeNotCaughtUp,
}

/// Consensus control record for voter set changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VotersRecordPayload {
    pub record: VotersRecord,
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
