use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Information about a voter in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: String,
}

/// A voters record describing the current voter set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u64,
    pub voters: Vec<VoterInfo>,
}
