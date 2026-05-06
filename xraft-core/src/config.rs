use std::path::PathBuf;

/// Construction-time configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub fetch_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_fetch_bytes: u32,
    /// Number of committed entries between automatic snapshots.
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 256,
            max_fetch_bytes: 1024 * 1024, // 1 MiB
            snapshot_interval: 10_000,
            data_dir: PathBuf::from("data"),
        }
    }
}

impl RaftConfig {
    /// Validate the Raft timing invariant:
    /// `fetch_interval < election_timeout_min < election_timeout_max`.
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err("election_timeout_min must be < election_timeout_max".into());
        }
        if self.fetch_interval_ms >= self.election_timeout_min_ms {
            return Err("fetch_interval must be < election_timeout_min".into());
        }
        if self.snapshot_interval == 0 {
            return Err("snapshot_interval must be > 0".into());
        }
        Ok(())
    }
}
