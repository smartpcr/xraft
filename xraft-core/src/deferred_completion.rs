//! DeferredCompletionQueue — parks proposal futures until HW advances.
//!
//! Per architecture §3.2, when `propose()` is called the client receives a
//! `oneshot::Receiver<u64>` that resolves when the entry's offset is committed
//! (i.e., offset < HW). The EventLoop calls `complete(hw)` after each HW
//! advancement, firing all oneshot senders whose offset < hw.

use std::collections::BTreeMap;
use tokio::sync::oneshot;

/// Parks `tokio::sync::oneshot` senders keyed by log offset.
///
/// When the high watermark advances, the queue completes all futures whose
/// offset is now < HW (strictly less than — per §3.1 canonical HW definition).
/// Analogous to KRaft's DeferredEventQueue / purgatory.
pub struct DeferredCompletionQueue {
    pending: BTreeMap<u64, oneshot::Sender<u64>>,
}

impl DeferredCompletionQueue {
    pub fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
        }
    }

    /// Register a proposal at the given offset. Returns a receiver that
    /// resolves with the committed offset when HW advances past it.
    pub fn enqueue(&mut self, offset: u64) -> oneshot::Receiver<u64> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(offset, tx);
        rx
    }

    /// Complete all pending proposals with offsets strictly less than `hw`.
    /// Sends the committed offset through each oneshot channel.
    pub fn complete(&mut self, hw: u64) {
        // Collect offsets to complete (split_off gives us keys >= hw, we keep < hw)
        let remaining = self.pending.split_off(&hw);
        let to_complete = std::mem::replace(&mut self.pending, remaining);
        for (offset, sender) in to_complete {
            // Ignore send errors — receiver may have been dropped
            let _ = sender.send(offset);
        }
    }

    /// Number of proposals still waiting for commit.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Check if all tracked proposals have been completed.
    pub fn all_completed(&self) -> bool {
        self.pending.is_empty()
    }
}

impl Default for DeferredCompletionQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_and_complete() {
        let mut dcq = DeferredCompletionQueue::new();
        let mut rx1 = dcq.enqueue(0);
        let mut rx2 = dcq.enqueue(1);
        let mut rx3 = dcq.enqueue(2);

        assert_eq!(dcq.pending_count(), 3);

        // HW=2 completes offsets 0 and 1 (< 2), not offset 2
        dcq.complete(2);
        assert_eq!(dcq.pending_count(), 1);

        assert_eq!(rx1.try_recv().unwrap(), 0);
        assert_eq!(rx2.try_recv().unwrap(), 1);
        assert!(rx3.try_recv().is_err()); // still pending

        // HW=3 completes offset 2
        dcq.complete(3);
        assert!(dcq.all_completed());
        assert_eq!(rx3.try_recv().unwrap(), 2);
    }

    #[test]
    fn complete_idempotent_when_empty() {
        let mut dcq = DeferredCompletionQueue::new();
        dcq.complete(100); // should not panic
        assert!(dcq.all_completed());
    }
}
