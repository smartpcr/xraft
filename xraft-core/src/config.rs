/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub fetch_interval_ms: u64,
    pub max_batch_size: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 100,
        }
    }
}
