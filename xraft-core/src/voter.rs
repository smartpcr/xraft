use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Information about a voter node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: SocketAddr,
}

/// A complete voter set record, committed via the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u64,
    pub voters: Vec<VoterInfo>,
}
