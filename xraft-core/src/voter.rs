use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Information about a single voter in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: SocketAddr,
}

/// A snapshot of the current voter set, committed as a control record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u64,
    pub voters: Vec<VoterInfo>,
}
