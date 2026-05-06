use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Network address for a voter node (architecture §3.1: `SocketAddr`).
pub type Endpoint = SocketAddr;

/// Identity and endpoint of a single voter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
}

/// Complete voter-set snapshot, committed via the log as a control entry.
///
/// Encodes the **full** new voter set (not a delta). On commit, the
/// in-memory voter set is atomically replaced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u32,
    pub voters: Vec<VoterInfo>,
}
