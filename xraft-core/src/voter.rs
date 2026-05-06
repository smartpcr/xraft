use crate::types::NodeId;

/// Information about a voter in the cluster.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: String,
}

/// A voters record, stored as a control entry in the log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VotersRecord {
    pub version: u64,
    pub voters: Vec<VoterInfo>,
}
