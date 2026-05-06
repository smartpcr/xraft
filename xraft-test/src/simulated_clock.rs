//! Deterministic clock for simulation testing.
//!
//! Implements the `Clock` trait to provide explicit time control in tests.
//! The clock is advanced by the test harness, ensuring reproducible behavior.

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use xraft_core::traits::Clock;

/// Deterministic simulated clock implementing the `Clock` trait.
///
/// Time advances only when explicitly told to via `advance()` or `set()`.
/// Election timeout is deterministic but configurable per-instance to
/// support node-specific timeout staggering.
pub struct SimulatedClock {
    inner: Arc<Mutex<ClockInner>>,
}

struct ClockInner {
    now_ms: u64,
    election_timeout_ms: u64,
}

impl SimulatedClock {
    /// Create a new clock starting at time 0 with the given election timeout.
    pub fn new(election_timeout_ms: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClockInner {
                now_ms: 0,
                election_timeout_ms,
            })),
        }
    }

    /// Advance the clock by `ms` milliseconds.
    pub fn advance(&self, ms: u64) {
        self.inner.lock().unwrap().now_ms += ms;
    }

    /// Set the clock to an absolute value.
    pub fn set(&self, ms: u64) {
        self.inner.lock().unwrap().now_ms = ms;
    }

    /// Read the current time without advancing.
    pub fn now(&self) -> u64 {
        self.inner.lock().unwrap().now_ms
    }
}

#[async_trait]
impl Clock for SimulatedClock {
    fn now_ms(&self) -> u64 {
        self.inner.lock().unwrap().now_ms
    }

    fn random_election_timeout_ms(&self) -> u64 {
        self.inner.lock().unwrap().election_timeout_ms
    }
}
