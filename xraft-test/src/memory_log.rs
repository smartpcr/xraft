use std::sync::Mutex;

use async_trait::async_trait;
use xraft_core::error::Result;
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;

/// In-memory log store for testing. Thread-safe via interior `Mutex`.
pub struct MemoryLogStore {
    inner: Mutex<MemoryLogInner>,
}

struct MemoryLogInner {
    entries: Vec<LogEntry>,
    start_offset: u64,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(MemoryLogInner {
                entries: Vec::new(),
                start_offset: 0,
            }),
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
        let mut inner = self.inner.lock().unwrap();
        for entry in entries {
            inner.entries.push(entry.clone());
        }
        Ok(())
    }

    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>> {
        let inner = self.inner.lock().unwrap();
        let result = inner
            .entries
            .iter()
            .filter(|e| e.offset >= start_offset && e.offset < end_offset)
            .cloned()
            .collect();
        Ok(result)
    }

    async fn truncate_suffix(&self, from_offset: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.retain(|e| e.offset < from_offset);
        Ok(())
    }

    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.retain(|e| e.offset >= up_to_offset);
        inner.start_offset = up_to_offset;
        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        self.inner.lock().unwrap().start_offset
    }

    fn log_end_offset(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner
            .entries
            .last()
            .map(|e| e.offset + 1)
            .unwrap_or(inner.start_offset)
    }

    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.iter().find(|e| e.offset == offset).cloned())
    }
}
