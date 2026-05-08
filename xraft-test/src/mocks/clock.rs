use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use xraft_core::traits::Clock;

pub struct MockClock {
    base: Instant,
    offset_ms: AtomicU64,
    election_timeout: Mutex<Duration>,
}

impl MockClock {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            offset_ms: AtomicU64::new(0),
            election_timeout: Mutex::new(Duration::from_millis(200)),
        }
    }

    pub fn set_election_timeout(&self, d: Duration) {
        *self.election_timeout.lock().unwrap() = d;
    }

    pub fn advance(&self, d: Duration) {
        self.offset_ms
            .fetch_add(d.as_millis() as u64, Ordering::SeqCst);
    }
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for MockClock {
    fn now(&self) -> Instant {
        self.base + Duration::from_millis(self.offset_ms.load(Ordering::SeqCst))
    }

    async fn sleep_until(&self, deadline: Instant) {
        let now = self.now();
        if deadline > now {
            let diff = deadline - now;
            self.advance(diff);
        }
    }

    fn random_election_timeout(&self) -> Duration {
        *self.election_timeout.lock().unwrap()
    }
}

/// Allows sharing a MockClock between the test and the RaftNode via Arc.
pub struct SharedMockClock(pub Arc<MockClock>);

impl SharedMockClock {
    pub fn new() -> (Self, Arc<MockClock>) {
        let clock = Arc::new(MockClock::new());
        (Self(Arc::clone(&clock)), clock)
    }
}

#[async_trait]
impl Clock for SharedMockClock {
    fn now(&self) -> Instant {
        self.0.now()
    }

    async fn sleep_until(&self, deadline: Instant) {
        self.0.sleep_until(deadline).await;
    }

    fn random_election_timeout(&self) -> Duration {
        self.0.random_election_timeout()
    }
}
