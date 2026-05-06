use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Information about a voter in the Raft cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
}
