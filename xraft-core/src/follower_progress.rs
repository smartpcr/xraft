use crate::types::NodeId;

/// Tracks a follower's replication progress on the leader side.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    pub fetch_offset: u64,
    pub last_fetch_timestamp: tokio::time::Instant,
    pub is_voter: bool,
}
