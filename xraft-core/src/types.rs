use std::fmt;
use std::net::SocketAddr;

/// Unique numeric identifier for a node within the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Monotonically increasing logical clock (epoch).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Term(pub u64);

/// Position in the log (0-indexed).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Offset(pub u64);

/// Position in the log (0-indexed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Offset(pub u64);

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Offset({})", self.0)
    }
}

/// Cluster identity for fencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub uuid::Uuid);

impl ClusterId {
    pub fn new() -> Self {
        ClusterId(uuid::Uuid::new_v4())
    }
}

impl Default for ClusterId {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a voter in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: SocketAddr,
}

/// A complete voter set record. Committed via the log as a control entry.
/// Encodes the **complete** new voter set (not a delta).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u32,
    pub voters: Vec<VoterInfo>,
}

impl VotersRecord {
    /// Returns true if the given node_id is in the voter set.
    pub fn contains(&self, node_id: NodeId) -> bool {
        self.voters.iter().any(|v| v.node_id == node_id)
    }

    /// Returns the VoterInfo for the given node_id, if present.
    pub fn get(&self, node_id: NodeId) -> Option<&VoterInfo> {
        self.voters.iter().find(|v| v.node_id == node_id)
    }
}

/// Cluster identity for fencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub uuid::Uuid);

impl Default for ClusterId {
    fn default() -> Self {
        ClusterId(uuid::Uuid::new_v4())
    }
}

/// Log position (0-indexed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct Offset(pub u64);
