use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::time::Instant;
use xraft_core::traits::Clock;

/// Deterministic simulated clock for testing.
///
/// Advances only when explicitly ticked via `advance(duration)`.
/// Allows precise control over election timeouts, fetch intervals,
/// and Check Quorum deadlines.
pub struct SimulatedClock {
    /// The real instant when this clock was created (anchor point).
    base: Instant,
    /// Sender side of the watch channel: broadcasts current simulated elapsed.
    elapsed_tx: watch::Sender<Duration>,
    /// Receiver side for sleep_until waiters.
    elapsed_rx: watch::Receiver<Duration>,
    /// Configurable election timeout to return from `random_election_timeout`.
    election_timeout: Mutex<Duration>,
}

impl SimulatedClock {
    /// Create a new `SimulatedClock` starting at time 0 (relative to creation).
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(Duration::ZERO);
        Self {
            base: Instant::now(),
            elapsed_tx: tx,
            elapsed_rx: rx,
            election_timeout: Mutex::new(Duration::from_millis(150)),
        }
    }

    /// Advance the simulated clock by the given duration.
    /// All pending `sleep_until` calls whose deadline is now in the past
    /// will be woken.
    pub fn advance(&self, duration: Duration) {
        self.elapsed_tx.send_modify(|elapsed| {
            *elapsed += duration;
        });
    }

    /// Set the value returned by `random_election_timeout`.
    pub fn set_election_timeout(&self, timeout: Duration) {
        *self.election_timeout.lock().unwrap() = timeout;
    }

    /// Get the current simulated elapsed time since creation.
    pub fn elapsed(&self) -> Duration {
        *self.elapsed_rx.borrow()
    }
}

impl Default for SimulatedClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for SimulatedClock {
    fn now(&self) -> Instant {
        self.base + *self.elapsed_rx.borrow()
    }

    async fn sleep_until(&self, deadline: Instant) {
        // If deadline is already in the past, return immediately
        if deadline <= self.now() {
            return;
        }

        let mut rx = self.elapsed_rx.clone();
        loop {
            // Check if we've passed the deadline
            let current = self.base + *rx.borrow();
            if current >= deadline {
                return;
            }

            // Wait for the next time advance
            if rx.changed().await.is_err() {
                // Channel closed — the clock was dropped
                return;
            }
        }
    }

    fn random_election_timeout(&self) -> Duration {
        *self.election_timeout.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_simulated_clock_starts_at_zero() {
        let clock = SimulatedClock::new();
        let base = clock.base;
        assert_eq!(clock.now(), base);
        assert_eq!(clock.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_simulated_clock_advance() {
        let clock = SimulatedClock::new();
        let base = clock.base;

        clock.advance(Duration::from_millis(150));

        assert_eq!(clock.now(), base + Duration::from_millis(150));
        assert_eq!(clock.elapsed(), Duration::from_millis(150));
    }

    #[tokio::test]
    async fn test_simulated_clock_advance_150ms_no_wall_clock() {
        // Scenario: Given a SimulatedClock at time 0, When advanced by 150 ms,
        // Then now() returns 150 ms and no wall-clock time has passed
        let wall_start = std::time::Instant::now();
        let clock = SimulatedClock::new();
        let base = clock.base;

        clock.advance(Duration::from_millis(150));

        assert_eq!(clock.now(), base + Duration::from_millis(150));
        assert_eq!(clock.elapsed(), Duration::from_millis(150));

        let wall_elapsed = wall_start.elapsed();
        // Should be well under 10ms of wall-clock time
        assert!(
            wall_elapsed < Duration::from_millis(10),
            "wall clock elapsed {wall_elapsed:?} — should be near-instant"
        );
    }

    #[tokio::test]
    async fn test_simulated_clock_sleep_until_already_past() {
        let clock = SimulatedClock::new();
        clock.advance(Duration::from_millis(100));

        let deadline = clock.base + Duration::from_millis(50);
        // Should return immediately
        clock.sleep_until(deadline).await;
    }

    #[tokio::test]
    async fn test_simulated_clock_sleep_until_wakes_on_advance() {
        let clock = Arc::new(SimulatedClock::new());
        let clock2 = clock.clone();
        let deadline = clock.base + Duration::from_millis(200);

        let handle = tokio::spawn(async move {
            clock2.sleep_until(deadline).await;
        });

        // Give the spawned task a moment to start waiting
        tokio::task::yield_now().await;

        // Advance past the deadline
        clock.advance(Duration::from_millis(250));

        // The sleep should complete
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("sleep_until should have woken up")
            .expect("task should not panic");
    }

    #[tokio::test]
    async fn test_simulated_clock_incremental_advance() {
        let clock = Arc::new(SimulatedClock::new());
        let clock2 = clock.clone();
        let deadline = clock.base + Duration::from_millis(300);

        let handle = tokio::spawn(async move {
            clock2.sleep_until(deadline).await;
        });

        tokio::task::yield_now().await;

        // Advance in steps — sleep shouldn't wake until past deadline
        clock.advance(Duration::from_millis(100));
        tokio::task::yield_now().await;

        clock.advance(Duration::from_millis(100));
        tokio::task::yield_now().await;

        // Still at 200ms, deadline is 300ms — should still be sleeping
        // Now cross the deadline
        clock.advance(Duration::from_millis(150));

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("sleep_until should have woken up")
            .expect("task should not panic");

        assert_eq!(clock.elapsed(), Duration::from_millis(350));
    }

    #[tokio::test]
    async fn test_random_election_timeout_configurable() {
        let clock = SimulatedClock::new();
        assert_eq!(
            clock.random_election_timeout(),
            Duration::from_millis(150)
        );

        clock.set_election_timeout(Duration::from_millis(300));
        assert_eq!(
            clock.random_election_timeout(),
            Duration::from_millis(300)
        );
    }

    #[tokio::test]
    async fn test_multiple_advances() {
        let clock = SimulatedClock::new();
        let base = clock.base;

        clock.advance(Duration::from_millis(50));
        clock.advance(Duration::from_millis(100));
        clock.advance(Duration::from_millis(25));

        assert_eq!(clock.now(), base + Duration::from_millis(175));
    }
}
