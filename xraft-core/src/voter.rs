use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::types::NodeId;

/// Information about a voter node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: String,
}

/// Membership change control record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u64,
    pub voters: Vec<VoterInfo>,
}
