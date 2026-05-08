use std::path::PathBuf;

/// Configuration for the Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's unique identifier within the cluster.
    pub node_id: NodeId,
    /// Lower bound for randomised election timeout (ms).
    pub election_timeout_min_ms: u64,
    /// Upper bound for randomised election timeout (ms).
    pub election_timeout_max_ms: u64,
    /// Follower's periodic Fetch RPC interval (ms).
    pub fetch_interval_ms: u64,
    /// Max entries drained from BatchAccumulator per tick.
    pub max_batch_size: usize,
    /// Max response payload for a single Fetch RPC.
    pub max_fetch_bytes: u32,
    /// Committed entries between automatic snapshots.
    pub snapshot_interval: u64,
    /// Root directory for log segments, snapshots, quorum-state.
    pub data_dir: PathBuf,
}

impl RaftConfig {
    /// Create a config with the given node_id and all other fields defaulted.
    pub fn with_node_id(node_id: NodeId) -> Self {
        Self {
            node_id,
            ..Self::default_inner()
        }
    }

    fn default_inner() -> Self {
        Self {
            node_id: NodeId(0),
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 256,
            max_fetch_bytes: 1_048_576, // 1 MiB
            snapshot_interval: 10_000,
            data_dir: PathBuf::from("data"),
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self::default_inner()
    }
}

impl RaftConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err(
                "election_timeout_min_ms must be < election_timeout_max_ms".to_string(),
            );
        }
        if self.fetch_interval_ms >= self.election_timeout_min_ms {
            return Err("fetch_interval_ms must be < election_timeout_min_ms".to_string());
        }
        Ok(())
    }
}
