use serde::{Deserialize, Serialize};

use crate::types::{ClusterId, NodeId, Term};

/// Persisted voting state — written to the `quorum-state` file.
///
/// Separated from the log for bootstrapping and performance.
/// Recovery code interprets `None` (file missing) as initial state
/// with `term=0` and no vote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: Term,
    /// Cluster identity, persisted for recovery after restart.
    #[serde(default = "default_cluster_id")]
    pub cluster_id: ClusterId,
}

fn default_cluster_id() -> ClusterId {
    ClusterId(uuid::Uuid::nil())
}
