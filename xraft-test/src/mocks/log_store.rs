use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;

pub struct MockLogStore {
    entries: Mutex<Vec<LogEntry>>,
    start_offset: AtomicU64,
    append_count: AtomicU64,
}

impl MockLogStore {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            start_offset: AtomicU64::new(0),
            append_count: AtomicU64::new(0),
        }
    }

    /// Creates a log store that appears non-empty.
    pub fn with_end_offset(end: u64) -> Self {
        let mut entries = Vec::new();
        for i in 0..end {
            entries.push(LogEntry {
                offset: i,
                term: xraft_core::types::Term(1),
                entry_type: xraft_core::log_entry::EntryType::Command,
                payload: bytes::Bytes::new(),
            });
        }
        Self {
            entries: Mutex::new(entries),
            start_offset: AtomicU64::new(0),
            append_count: AtomicU64::new(0),
        }
    }

    pub fn append_count(&self) -> u64 {
        self.append_count.load(Ordering::SeqCst)
    }
}

impl Default for MockLogStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LogStore for MockLogStore {
    async fn append(&self, entries: &[LogEntry]) -> std::io::Result<()> {
        self.append_count.fetch_add(1, Ordering::SeqCst);
        let mut store = self.entries.lock().unwrap();
        store.extend(entries.iter().cloned());
        Ok(())
    }

    async fn read(&self, start: u64, end: u64) -> std::io::Result<Vec<LogEntry>> {
        let store = self.entries.lock().unwrap();
        let s = self.start_offset.load(Ordering::SeqCst);
        Ok(store
            .iter()
            .filter(|e| e.offset >= start && e.offset < end && e.offset >= s)
            .cloned()
            .collect())
    }

    async fn truncate_suffix(&self, from: u64) -> std::io::Result<()> {
        let mut store = self.entries.lock().unwrap();
        store.retain(|e| e.offset < from);
        Ok(())
    }

    async fn truncate_prefix(&self, up_to: u64) -> std::io::Result<()> {
        self.start_offset.store(up_to, Ordering::SeqCst);
        let mut store = self.entries.lock().unwrap();
        store.retain(|e| e.offset >= up_to);
        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        self.start_offset.load(Ordering::SeqCst)
    }

    fn log_end_offset(&self) -> u64 {
        let store = self.entries.lock().unwrap();
        store.last().map_or(
            self.start_offset.load(Ordering::SeqCst),
            |e| e.offset + 1,
        )
    }

    async fn entry_at(&self, offset: u64) -> std::io::Result<Option<LogEntry>> {
        let store = self.entries.lock().unwrap();
        Ok(store.iter().find(|e| e.offset == offset).cloned())
    }
}
