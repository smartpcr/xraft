use serde::{Deserialize, Serialize};

use crate::types::{NodeId, Term};

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
}
