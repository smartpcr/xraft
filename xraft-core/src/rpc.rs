use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Request to add a new voter to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddVoterRequest {
    pub node_id: NodeId,
    pub endpoint: SocketAddr,
}

/// Request to remove a voter from the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveVoterRequest {
    pub node_id: NodeId,
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
