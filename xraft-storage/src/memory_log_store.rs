//! In-memory implementation of the `LogStore` trait.
//!
//! Fast, deterministic, and suitable for testing. Supports optional fault
//! injection (configurable `fsync` failure probability, write corruption).

use std::sync::RwLock;

use async_trait::async_trait;
use xraft_core::error::{Result, XraftError};
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;

/// Internal mutable state guarded by a `RwLock`.
#[derive(Debug)]
struct Inner {
    entries: Vec<LogEntry>,
    start_offset: u64,
    /// When > 0.0, `append` returns `StorageError` with this probability.
    fsync_failure_probability: f64,
}

/// In-memory log store implementing `LogStore`.
///
/// Uses `RwLock` for interior mutability so the `&self` trait methods can
/// mutate state. All operations are instantaneous (no real I/O).
#[derive(Debug)]
pub struct MemoryLogStore {
    inner: RwLock<Inner>,
}

impl MemoryLogStore {
    /// Create an empty log store starting at offset 0.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                entries: Vec::new(),
                start_offset: 0,
                fsync_failure_probability: 0.0,
            }),
        }
    }

    /// Set the fsync failure probability (0.0 = never, 1.0 = always).
    pub fn set_fsync_failure_probability(&self, prob: f64) {
        self.inner.write().unwrap().fsync_failure_probability = prob;
    }

    // -- Synchronous accessors for use by test harnesses --------------------

    /// Return a clone of all entries currently in the log.
    pub fn entries(&self) -> Vec<LogEntry> {
        self.inner.read().unwrap().entries.clone()
    }

    /// Synchronous append — same as the async trait method but blocking.
    pub fn append_sync(&self, entries: &[LogEntry]) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        if inner.fsync_failure_probability >= 1.0 {
            return Err(XraftError::StorageError(
                "simulated fsync failure".into(),
            ));
        }
        for entry in entries {
            inner.entries.push(entry.clone());
        }
        Ok(())
    }

    /// Synchronous read — same as the async trait method but blocking.
    pub fn read_sync(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>> {
        let inner = self.inner.read().unwrap();
        let start_idx = start_offset.saturating_sub(inner.start_offset) as usize;
        let end_idx = end_offset.saturating_sub(inner.start_offset) as usize;
        let end_idx = end_idx.min(inner.entries.len());
        if start_idx > end_idx {
            return Ok(Vec::new());
        }
        Ok(inner.entries[start_idx..end_idx].to_vec())
    }

    /// Synchronous entry lookup.
    pub fn entry_at_sync(&self, offset: u64) -> Result<Option<LogEntry>> {
        let inner = self.inner.read().unwrap();
        if offset < inner.start_offset {
            return Ok(None);
        }
        let idx = (offset - inner.start_offset) as usize;
        Ok(inner.entries.get(idx).cloned())
    }

    /// Synchronous truncate suffix.
    pub fn truncate_suffix_sync(&self, from_offset: u64) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        if from_offset <= inner.start_offset {
            inner.entries.clear();
        } else {
            let idx = (from_offset - inner.start_offset) as usize;
            inner.entries.truncate(idx);
        }
        Ok(())
    }

    /// Synchronous truncate prefix (log compaction).
    pub fn truncate_prefix_sync(&self, up_to_offset: u64) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        if up_to_offset <= inner.start_offset {
            return Ok(());
        }
        let remove_count = (up_to_offset - inner.start_offset) as usize;
        if remove_count >= inner.entries.len() {
            inner.entries.clear();
            inner.start_offset = up_to_offset;
        } else {
            inner.entries.drain(..remove_count);
            inner.start_offset = up_to_offset;
        }
        Ok(())
    }

    /// Overwrite an entry at a specific offset. Used for fault injection
    /// (simulating a buggy `ReplicationManager`).
    pub fn overwrite_entry(&self, offset: u64, entry: LogEntry) {
        let mut inner = self.inner.write().unwrap();
        let idx = (offset - inner.start_offset) as usize;
        if idx < inner.entries.len() {
            inner.entries[idx] = entry;
        }
    }
}

impl Default for MemoryLogStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LogStore for MemoryLogStore {
    async fn append(&self, entries: &[LogEntry]) -> Result<()> {
        self.append_sync(entries)
    }

    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>> {
        self.read_sync(start_offset, end_offset)
    }

    async fn truncate_suffix(&self, from_offset: u64) -> Result<()> {
        self.truncate_suffix_sync(from_offset)
    }

    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()> {
        self.truncate_prefix_sync(up_to_offset)
    }

    fn log_start_offset(&self) -> u64 {
        self.inner.read().unwrap().start_offset
    }

    fn log_end_offset(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.start_offset + inner.entries.len() as u64
    }

    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>> {
        self.entry_at_sync(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::log_entry::LogEntry;
    use xraft_core::types::{Offset, Term};

    fn cmd(offset: u64, term: u64, data: &[u8]) -> LogEntry {
        LogEntry::command(Offset(offset), Term(term), data.to_vec())
    }

    #[test]
    fn append_and_read_back() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..100)
            .map(|i| cmd(i, 1, &i.to_le_bytes()))
            .collect();
        store.append_sync(&entries).unwrap();

        assert_eq!(store.log_start_offset(), 0);
        assert_eq!(store.log_end_offset(), 100);

        let readback = store.read_sync(0, 100).unwrap();
        assert_eq!(readback.len(), 100);
        for (i, e) in readback.iter().enumerate() {
            assert_eq!(e.offset.0, i as u64);
            assert_eq!(e.term.0, 1);
        }
    }

    #[test]
    fn entry_at_returns_correct_entry() {
        let store = MemoryLogStore::new();
        store
            .append_sync(&[cmd(0, 1, b"a"), cmd(1, 1, b"b")])
            .unwrap();
        let e = store.entry_at_sync(1).unwrap().unwrap();
        assert_eq!(e.payload, b"b");
        assert!(store.entry_at_sync(5).unwrap().is_none());
    }

    #[test]
    fn truncate_suffix_removes_tail() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..10).map(|i| cmd(i, 1, b"x")).collect();
        store.append_sync(&entries).unwrap();
        store.truncate_suffix_sync(5).unwrap();
        assert_eq!(store.log_end_offset(), 5);
        assert!(store.entry_at_sync(5).unwrap().is_none());
    }

    #[test]
    fn truncate_prefix_compacts() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..10).map(|i| cmd(i, 1, b"x")).collect();
        store.append_sync(&entries).unwrap();
        store.truncate_prefix_sync(5).unwrap();
        assert_eq!(store.log_start_offset(), 5);
        assert_eq!(store.log_end_offset(), 10);
        assert!(store.entry_at_sync(3).unwrap().is_none());
        assert!(store.entry_at_sync(5).unwrap().is_some());
    }

    #[test]
    fn fsync_failure_injection() {
        let store = MemoryLogStore::new();
        store.set_fsync_failure_probability(1.0);
        let result = store.append_sync(&[cmd(0, 1, b"x")]);
        assert!(result.is_err());
    }
}
