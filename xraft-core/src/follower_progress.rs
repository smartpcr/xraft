use crate::types::NodeId;
use tokio::time::Instant;

/// Leader-side per-follower replication progress.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    pub fetch_offset: u64,
    pub last_fetch_timestamp: Instant,
    pub is_voter: bool,
}
