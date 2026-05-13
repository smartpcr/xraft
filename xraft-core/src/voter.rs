use std::net::SocketAddr;

use crate::types::NodeId;

/// Information about a voter in the Raft cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: u64,
    pub endpoint: String,
}
