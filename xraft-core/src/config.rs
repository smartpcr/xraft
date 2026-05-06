use std::path::PathBuf;
use std::time::Duration;

/// Construction-time configuration for a RaftNode.
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
    /// Max response payload for a single Fetch RPC (default: 1 MiB).
    pub max_fetch_bytes: u32,
    /// Committed entries between automatic snapshots (default: 10_000).
    pub snapshot_interval: u64,
    /// Root directory for log segments, snapshots, quorum-state.
    pub data_dir: PathBuf,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            fetch_interval: Duration::from_millis(50),
            max_batch_size: 256,
            max_fetch_bytes: 1024 * 1024,
            snapshot_interval: 10_000,
            data_dir: PathBuf::from("data"),
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
