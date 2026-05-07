use serde::{Deserialize, Serialize};

/// Information about a voter in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: u64,
    pub endpoint: String,
}

/// A record of the current voter set, appended to the log on membership changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u32,
    pub voters: Vec<VoterInfo>,
}
