use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::types::NodeId;

/// Information about a voter in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: SocketAddr,
}

/// Encodes the complete new voter set (not a delta).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u32,
    pub voters: Vec<VoterInfo>,
}
