use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Per-follower replication progress tracked by the leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    pub fetch_offset: u64,
    /// Whether this follower counts for quorum.
    pub is_voter: bool,
}
