use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use xraft_core::traits::Clock;

/// Deterministic simulated clock for testing.
/// Time only advances via explicit `advance()` calls.
pub struct SimulatedClock {
    inner: Arc<Mutex<SimulatedClockInner>>,
}

struct SimulatedClockInner {
    now: Instant,
    election_timeout: Duration,
}

impl SimulatedClock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimulatedClockInner {
                now: Instant::now(),
                election_timeout: Duration::from_millis(200),
            })),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let mut inner = self.inner.lock().unwrap();
        inner.now += duration;
    }

    pub fn set_election_timeout(&self, timeout: Duration) {
        let mut inner = self.inner.lock().unwrap();
        inner.election_timeout = timeout;
    }
}

impl Clock for SimulatedClock {
    fn now(&self) -> Instant {
        self.inner.lock().unwrap().now
    }

    fn random_election_timeout(&self) -> Duration {
        self.inner.lock().unwrap().election_timeout
    }
}

impl Default for SimulatedClock {
    fn default() -> Self {
        Self::new()
    }
}
