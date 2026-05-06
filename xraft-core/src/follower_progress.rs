use crate::types::NodeId;
use tokio::time::Instant;

/// Per-follower replication progress tracked on the leader side.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    /// The next offset this follower wants to read (= follower's log_end_offset).
    pub fetch_offset: u64,
    /// When this follower last sent a Fetch.
    pub last_fetch_timestamp: Instant,
    /// Whether this follower counts for quorum.
    pub is_voter: bool,
}
