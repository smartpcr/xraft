use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use xraft_core::traits::Clock;

/// Deterministic clock for testing. Time only advances when
/// `advance()` is called. `sleep_until` returns immediately when
/// the internal instant is >= the requested deadline, otherwise
/// it yields once and returns (tests must advance time externally).
#[derive(Clone)]
pub struct SimulatedClock {
    inner: Arc<Mutex<SimulatedClockInner>>,
}

struct SimulatedClockInner {
    now: Instant,
    election_timeout: Duration,
}

impl SimulatedClock {
    pub fn new(election_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimulatedClockInner {
                now: Instant::now(),
                election_timeout,
            })),
        }
    }

    /// Advance clock by the given duration.
    pub fn advance(&self, d: Duration) {
        let mut inner = self.inner.lock().unwrap();
        inner.now += d;
    }

    /// Get current simulated instant.
    pub fn current(&self) -> Instant {
        self.inner.lock().unwrap().now
    }
}

#[async_trait]
impl Clock for SimulatedClock {
    fn now(&self) -> Instant {
        self.inner.lock().unwrap().now
    }

    async fn sleep_until(&self, deadline: Instant) {
        // In tests we don't actually sleep — we just yield once.
        // The caller is expected to advance() the clock past the deadline.
        if self.now() < deadline {
            tokio::task::yield_now().await;
        }
    }

    fn random_election_timeout(&self) -> Duration {
        // Deterministic in tests.
        self.inner.lock().unwrap().election_timeout
    }
}
