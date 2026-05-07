use tokio::time::Instant;

use crate::types::NodeId;

/// Leader-side per-follower replication progress tracking.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    pub fetch_offset: u64,
    /// `None` means no Fetch has ever been received from this follower.
    pub last_fetch_timestamp: Option<Instant>,
    pub is_voter: bool,
}
