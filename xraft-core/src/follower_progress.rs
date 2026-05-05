use tokio::time::Instant;

use crate::types::NodeId;

/// Leader-side per-follower replication progress tracker.
///
/// Not serialisable — this is transient, in-memory state maintained only
/// by the leader during its term.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    /// The next offset this follower wants to read (= follower's
    /// `log_end_offset`). The follower has replicated entries
    /// `[0, fetch_offset)`. Used directly in HW calculation.
    pub fetch_offset: u64,
    /// When this follower last sent a Fetch request.
    pub last_fetch_timestamp: Instant,
    /// Whether this follower counts for quorum.
    pub is_voter: bool,
}
