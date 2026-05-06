//! Deferred completion tracking for proposals.
//!
//! Simulates the `DeferredCompletionQueue` from the architecture: proposals
//! are tracked until the high watermark advances past their offset, at which
//! point they are considered committed.

/// Tracks pending proposals and resolves them when HW advances.
///
/// Models the `DeferredCompletionQueue` from architecture §3.2:
/// when `propose()` is called, the offset is registered as pending.
/// When `resolve(hw)` is called, all pending offsets below `hw` move
/// to the completed set. Tests can verify that all proposals eventually
/// complete via `all_completed()`.
#[derive(Debug, Default)]
pub struct CompletionTracker {
    pending: Vec<u64>,
    completed: Vec<u64>,
}

impl CompletionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a proposal offset as pending.
    pub fn track(&mut self, offset: u64) {
        self.pending.push(offset);
    }

    /// Resolve all pending proposals with offsets below `hw`.
    pub fn resolve(&mut self, hw: u64) {
        let mut still_pending = Vec::new();
        for offset in self.pending.drain(..) {
            if offset < hw {
                self.completed.push(offset);
            } else {
                still_pending.push(offset);
            }
        }
        self.pending = still_pending;
    }

    /// Check if all tracked proposals have been completed.
    pub fn all_completed(&self) -> bool {
        self.pending.is_empty()
    }

    /// Get the number of pending proposals.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get the number of completed proposals.
    pub fn completed_count(&self) -> usize {
        self.completed.len()
    }

    /// Get all completed offsets.
    pub fn completed_offsets(&self) -> &[u64] {
        &self.completed
    }
}
