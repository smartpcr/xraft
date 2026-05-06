use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Per-follower replication progress tracked by the leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    /// The next offset this follower wants to read (= follower's log_end_offset).
    pub fetch_offset: u64,
    /// Whether this follower counts for quorum.
    pub is_voter: bool,
}
