use std::path::PathBuf;

/// Construction-time configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Lower bound for randomised election timeout (default: 150ms).
    pub election_timeout_min: Duration,
    /// Upper bound for randomised election timeout (default: 300ms).
    pub election_timeout_max: Duration,
    /// Follower's periodic Fetch RPC interval (default: 50ms).
    pub fetch_interval: Duration,
    /// Max entries drained from BatchAccumulator per tick (default: 256).
    pub max_batch_size: usize,
    pub max_fetch_bytes: u32,
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
    /// Maximum segment file size in bytes before rolling to a new segment.
    pub segment_max_bytes: u64,
    /// Sparse index interval: write an index entry every N records.
    pub index_interval: u32,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 256,
            max_fetch_bytes: 1024 * 1024,
            snapshot_interval: 10_000,
            data_dir: PathBuf::from("data"),
            segment_max_bytes: 64 * 1024 * 1024, // 64 MiB
            index_interval: 256,
        }
    }
}

impl RaftConfig {
    /// Validate timing invariants per architecture spec.
    pub fn validate(&self) -> Result<(), String> {
        if self.fetch_interval >= self.election_timeout_min {
            return Err(format!(
                "fetch_interval ({:?}) must be < election_timeout_min ({:?})",
                self.fetch_interval, self.election_timeout_min
            ));
        }
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(format!(
                "election_timeout_min ({:?}) must be < election_timeout_max ({:?})",
                self.election_timeout_min, self.election_timeout_max
            ));
        }
        Ok(())
    }
}
