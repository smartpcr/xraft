use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::types::NodeId;

/// Information about a voter in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    /// Network address for RPC.
    pub endpoint: SocketAddr,
}

/// A complete voter set record. Committed via the log as a `LogEntry` with
/// `EntryType::VotersRecord`. Included in snapshot metadata for recovery.
/// The `voters` field encodes the **complete** new voter set (not a delta).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u32,
    pub voters: Vec<VoterInfo>,
}
