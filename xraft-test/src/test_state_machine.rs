//! Test implementation of the `StateMachine` trait for integration testing.
//!
//! Tracks all applied entries so tests can verify that the three-phase commit
//! notification (StateMachine::apply → Listener::handle_commit →
//! DeferredCompletionQueue::complete) delivers exactly the right entries in
//! the right order. Detects duplicate applies via full history tracking.

use std::collections::BTreeMap;
use xraft_core::traits::StateMachine;
use xraft_core::types::{AppRecord, AppSnapshot};

/// A test state machine that records every applied entry with full history.
///
/// Unlike a production state machine that only keeps current state, this
/// implementation preserves the complete application history, enabling
/// assertions about:
/// - Exactly-once delivery (no duplicate applies at the same offset)
/// - Monotonically increasing apply order
/// - Cross-node consistency (same data applied at same offset)
pub struct TestStateMachine {
    /// Applied entries keyed by offset — last-writer-wins for BTreeMap,
    /// but `applied_history` catches any duplicate attempts.
    applied: BTreeMap<u64, AppRecord>,
    /// Full ordered history of every apply() call: (offset, data).
    /// Used to detect duplicate applies that BTreeMap would mask.
    applied_history: Vec<(u64, AppRecord)>,
    /// Ordered list of offsets in application order.
    apply_order: Vec<u64>,
    /// Number of duplicate apply attempts detected.
    duplicate_apply_count: usize,
}

impl TestStateMachine {
    pub fn new() -> Self {
        Self {
            applied: BTreeMap::new(),
            applied_history: Vec::new(),
            apply_order: Vec::new(),
            duplicate_apply_count: 0,
        }
    }

    /// Reset the state machine (used on node restart when SM state is volatile).
    pub fn reset(&mut self) {
        self.applied.clear();
        self.applied_history.clear();
        self.apply_order.clear();
        self.duplicate_apply_count = 0;
    }

    /// Get all applied entries as a map from offset to AppRecord.
    pub fn applied_entries(&self) -> &BTreeMap<u64, AppRecord> {
        &self.applied
    }

    /// Get the number of unique entries applied to this state machine.
    pub fn applied_count(&self) -> usize {
        self.applied.len()
    }

    /// Get the full application history (including any duplicate attempts).
    pub fn applied_history(&self) -> &[(u64, AppRecord)] {
        &self.applied_history
    }

    /// Get the offsets in the order they were applied.
    pub fn apply_order(&self) -> &[u64] {
        &self.apply_order
    }

    /// Check if a specific offset has been applied.
    pub fn has_applied(&self, offset: u64) -> bool {
        self.applied.contains_key(&offset)
    }

    /// Get the data applied at a specific offset.
    pub fn get_applied(&self, offset: u64) -> Option<&AppRecord> {
        self.applied.get(&offset)
    }

    /// Get the highest applied offset, or None if nothing applied yet.
    pub fn highest_applied_offset(&self) -> Option<u64> {
        self.applied.keys().last().copied()
    }

    /// Get the count of duplicate apply attempts detected.
    pub fn duplicate_apply_count(&self) -> usize {
        self.duplicate_apply_count
    }
}

impl Default for TestStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine for TestStateMachine {
    fn apply(
        &mut self,
        offset: u64,
        record: &AppRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Track duplicate applies — BTreeMap::insert would silently overwrite
        if self.applied.contains_key(&offset) {
            self.duplicate_apply_count += 1;
        }
        self.applied_history.push((offset, record.clone()));
        self.applied.insert(offset, record.clone());
        self.apply_order.push(offset);
        Ok(())
    }

    fn snapshot(&self) -> Result<AppSnapshot, Box<dyn std::error::Error + Send + Sync>> {
        let data = self.applied.iter()
            .flat_map(|(offset, record)| {
                let mut v = offset.to_be_bytes().to_vec();
                v.extend_from_slice(&(record.data.len() as u32).to_be_bytes());
                v.extend_from_slice(&record.data);
                v
            })
            .collect();
        Ok(AppSnapshot { data })
    }

    fn restore(
        &mut self,
        snapshot: AppSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.applied.clear();
        self.applied_history.clear();
        self.apply_order.clear();
        self.duplicate_apply_count = 0;
        let mut cursor = 0;
        let data = &snapshot.data;
        while cursor + 12 <= data.len() {
            let offset = u64::from_be_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let len = u32::from_be_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + len > data.len() {
                break;
            }
            let record = AppRecord::new(data[cursor..cursor + len].to_vec());
            self.applied_history.push((offset, record.clone()));
            self.applied.insert(offset, record);
            self.apply_order.push(offset);
            cursor += len;
        }
        Ok(())
    }
}
