use std::path::PathBuf;

/// Configuration for a Raft node.
///
/// Per Stage 1.5 of the implementation plan, this struct holds the timing
/// parameters, batching limits, snapshot cadence, and on-disk data location
/// the consensus engine needs. The `node_id` and cluster identity are passed
/// separately to [`RaftNode::new`](../struct.RaftNode.html#method.new) and
/// [`RaftNode::bootstrap`](../struct.RaftNode.html#method.bootstrap) (Stage
/// 1.7), so they are deliberately **not** fields of `RaftConfig`.
///
/// All timing values are in milliseconds; durations are kept as plain
/// integers (rather than `std::time::Duration`) to make the config trivially
/// `Serialize`/`Deserialize`-able by later stages without pulling in
/// duration-aware serde helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftConfig {
    /// Lower bound for the randomised election timeout (milliseconds).
    ///
    /// A follower that has not received a valid Fetch response within a
    /// randomly chosen interval in `[election_timeout_min_ms,
    /// election_timeout_max_ms)` becomes a candidate and starts a new
    /// election.
    pub election_timeout_min_ms: u64,

    /// Upper bound (exclusive) for the randomised election timeout
    /// (milliseconds). Must be strictly greater than `election_timeout_min_ms`
    /// so that randomisation has a non-empty range.
    pub election_timeout_max_ms: u64,

    /// Periodic Fetch RPC interval used by followers to pull log entries from
    /// the leader (milliseconds). Must be strictly less than
    /// `election_timeout_min_ms` so that healthy followers can refresh their
    /// liveness signal before the election timer would otherwise fire.
    pub fetch_interval_ms: u64,

    /// Maximum number of entries the leader will drain from its batch
    /// accumulator in a single event-loop tick when building a Fetch response.
    pub max_batch_size: usize,

    /// Maximum payload size (in bytes) for a single Fetch RPC response. Acts
    /// as a network-side flow-control bound complementary to `max_batch_size`.
    pub max_fetch_bytes: u32,

    /// Number of committed entries between automatic snapshot triggers.
    pub snapshot_interval: u64,

    /// Root directory under which the storage layer places log segments,
    /// snapshots, and the persisted quorum-state file.
    pub data_dir: PathBuf,
}

impl Default for RaftConfig {
    /// Returns the canonical default configuration.
    ///
    /// The defaults are chosen so that they satisfy the Raft timing invariants
    /// enforced by [`RaftConfig::validate`]:
    /// `fetch_interval_ms (50)  <  election_timeout_min_ms (150)
    ///                            <  election_timeout_max_ms (300)`.
    fn default() -> Self {
        Self {
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

impl RaftConfig {
    /// Validate the configuration against the Raft timing invariants required
    /// for correctness:
    ///
    /// 1. `election_timeout_min_ms < election_timeout_max_ms` — the random
    ///    election timeout must be sampled from a non-empty range; if the
    ///    bounds are equal or inverted, every node would pick the same
    ///    timeout and split-vote storms become inevitable.
    /// 2. `fetch_interval_ms < election_timeout_min_ms` — followers must be
    ///    able to refresh their liveness contact with the leader before the
    ///    earliest possible election firing, otherwise even a healthy cluster
    ///    can churn through elections.
    ///
    /// Returns `Ok(())` on success or a descriptive error string on the first
    /// invariant that fails. The error is returned as `String` (rather than
    /// a [`crate::error::XraftError`] variant) because callers in later
    /// stages already wrap it into their own `InvalidConfig`-style variants
    /// once those stages are implemented.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err(format!(
                "election_timeout_min_ms ({}) must be < election_timeout_max_ms ({})",
                self.election_timeout_min_ms, self.election_timeout_max_ms
            ));
        }
        if self.fetch_interval_ms >= self.election_timeout_min_ms {
            return Err(format!(
                "fetch_interval_ms ({}) must be < election_timeout_min_ms ({})",
                self.fetch_interval_ms, self.election_timeout_min_ms
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = RaftConfig::default();
        cfg.validate()
            .expect("default config must satisfy Raft timing invariants");

        // The default must also satisfy the explicit Raft timing inequalities
        // (documented as part of the Stage 1.5 acceptance scenario).
        assert!(cfg.fetch_interval_ms < cfg.election_timeout_min_ms);
        assert!(cfg.election_timeout_min_ms < cfg.election_timeout_max_ms);
    }

    #[test]
    fn default_field_values() {
        let cfg = RaftConfig::default();
        assert_eq!(cfg.election_timeout_min_ms, 150);
        assert_eq!(cfg.election_timeout_max_ms, 300);
        assert_eq!(cfg.fetch_interval_ms, 50);
        assert_eq!(cfg.max_batch_size, 256);
        assert_eq!(cfg.max_fetch_bytes, 1_048_576);
        assert_eq!(cfg.snapshot_interval, 10_000);
        assert_eq!(cfg.data_dir, PathBuf::from("data"));
    }

    #[test]
    fn rejects_inverted_election_timeout_bounds() {
        let cfg = RaftConfig {
            election_timeout_min_ms: 500,
            election_timeout_max_ms: 100,
            ..RaftConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("inverted bounds must be rejected");
        assert!(err.contains("election_timeout_min_ms"));
        assert!(err.contains("election_timeout_max_ms"));
    }

    #[test]
    fn rejects_equal_election_timeout_bounds() {
        let cfg = RaftConfig {
            election_timeout_min_ms: 200,
            election_timeout_max_ms: 200,
            ..RaftConfig::default()
        };
        // Equal bounds collapse the randomisation window — also invalid.
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_fetch_interval_at_or_above_election_min() {
        let cfg = RaftConfig {
            fetch_interval_ms: 150,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            ..RaftConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("fetch_interval >= election_min must be rejected");
        assert!(err.contains("fetch_interval_ms"));
        assert!(err.contains("election_timeout_min_ms"));

        let cfg = RaftConfig {
            fetch_interval_ms: 200,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            ..RaftConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_custom_but_valid_timings() {
        let cfg = RaftConfig {
            election_timeout_min_ms: 1_000,
            election_timeout_max_ms: 2_000,
            fetch_interval_ms: 200,
            max_batch_size: 1_024,
            max_fetch_bytes: 8 * 1_048_576,
            snapshot_interval: 50_000,
            data_dir: PathBuf::from("/var/lib/xraft"),
        };
        assert!(cfg.validate().is_ok());
    }
}
