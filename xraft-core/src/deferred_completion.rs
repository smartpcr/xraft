use std::collections::BTreeMap;
use tokio::sync::oneshot;

/// Parks client futures keyed by log offset. When the high watermark advances,
/// the queue completes all futures whose offset is now < HW.
pub struct DeferredCompletionQueue {
    pending: BTreeMap<u64, Vec<oneshot::Sender<u64>>>,
}

impl DeferredCompletionQueue {
    pub fn new() -> Self {
        DeferredCompletionQueue {
            pending: BTreeMap::new(),
        }
    }

    /// Park a completion sender for the given offset.
    pub fn park(&mut self, offset: u64, sender: oneshot::Sender<u64>) {
        self.pending.entry(offset).or_default().push(sender);
    }

    /// Complete all entries with offset < high_watermark.
    /// Returns the number of completions fired.
    pub fn complete(&mut self, high_watermark: u64) -> usize {
        let mut count = 0;
        // Collect offsets to complete (offset < HW means committed).
        let offsets_to_complete: Vec<u64> = self
            .pending
            .range(..high_watermark)
            .map(|(&offset, _)| offset)
            .collect();

        for offset in offsets_to_complete {
            if let Some(senders) = self.pending.remove(&offset) {
                for sender in senders {
                    let _ = sender.send(offset);
                    count += 1;
                }
            }
        }
        count
    }

    /// Fail (drop) all pending completions with offset >= threshold.
    ///
    /// Used during divergence truncation: entries at or above the truncation
    /// point no longer exist, so their completions can never fire. Dropping
    /// the senders causes receivers to observe `RecvError`.
    /// Returns the number of completions failed.
    pub fn fail_at_or_above(&mut self, threshold: u64) -> usize {
        let mut count = 0;
        let offsets_to_fail: Vec<u64> = self
            .pending
            .range(threshold..)
            .map(|(&offset, _)| offset)
            .collect();

        for offset in offsets_to_fail {
            if let Some(senders) = self.pending.remove(&offset) {
                count += senders.len();
                // Senders are dropped here — receivers get RecvError.
            }
        }
        count
    }

    /// Number of pending completions.
    pub fn len(&self) -> usize {
        self.pending.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

impl Default for DeferredCompletionQueue {
    fn default() -> Self {
        Self::new()
    }
}
