//! In-memory implementation of the `LogStore` trait for testing.

use async_trait::async_trait;
use std::sync::RwLock;
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;

/// Thread-safe in-memory log store implementing the `LogStore` trait.
///
/// Uses `RwLock` for interior mutability to satisfy the `&self` requirement
/// on trait methods while remaining `Send + Sync`.
pub struct MemoryLogStore {
    entries: RwLock<Vec<LogEntry>>,
    start_offset: RwLock<u64>,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            start_offset: RwLock::new(0),
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
    async fn append(
        &self,
        entries: &[LogEntry],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut log = self.entries.write().unwrap();
        for entry in entries {
            log.push(entry.clone());
        }
        Ok(())
    }

    async fn read(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<LogEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let log = self.entries.read().unwrap();
        let result = log
            .iter()
            .filter(|e| e.offset >= start_offset && e.offset < end_offset)
            .cloned()
            .collect();
        Ok(result)
    }

    async fn truncate_suffix(
        &self,
        from_offset: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut log = self.entries.write().unwrap();
        log.retain(|e| e.offset < from_offset);
        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        *self.start_offset.read().unwrap()
    }

    fn log_end_offset(&self) -> u64 {
        let log = self.entries.read().unwrap();
        log.last().map(|e| e.offset + 1).unwrap_or_else(|| self.log_start_offset())
    }

    async fn entry_at(
        &self,
        offset: u64,
    ) -> Result<Option<LogEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let log = self.entries.read().unwrap();
        Ok(log.iter().find(|e| e.offset == offset).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::log_entry::LogEntry;
    use xraft_core::types::{AppRecord, Term};

    #[tokio::test]
    async fn append_and_read_back() {
        let store = MemoryLogStore::new();
        let mut entries = Vec::new();
        for i in 0..100u64 {
            entries.push(LogEntry::command(
                i,
                Term(1),
                &AppRecord::new(i.to_be_bytes().to_vec()),
            ));
        }
        store.append(&entries).await.unwrap();

        assert_eq!(store.log_end_offset(), 100);
        assert_eq!(store.log_start_offset(), 0);

        let read = store.read(0, 100).await.unwrap();
        assert_eq!(read.len(), 100);
        for (i, entry) in read.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
        }
    }

    #[tokio::test]
    async fn truncate_suffix_removes_tail() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..10u64)
            .map(|i| LogEntry::command(i, Term(1), &AppRecord::new(vec![i as u8])))
            .collect();
        store.append(&entries).await.unwrap();

        store.truncate_suffix(5).await.unwrap();
        assert_eq!(store.log_end_offset(), 5);

        let read = store.read(0, 10).await.unwrap();
        assert_eq!(read.len(), 5);
    }

    #[tokio::test]
    async fn entry_at_returns_correct_entry() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..5u64)
            .map(|i| LogEntry::command(i, Term(1), &AppRecord::new(vec![i as u8])))
            .collect();
        store.append(&entries).await.unwrap();

        let entry = store.entry_at(3).await.unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().offset, 3);

        let missing = store.entry_at(10).await.unwrap();
        assert!(missing.is_none());
    }
}
