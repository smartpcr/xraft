use std::sync::Mutex;

use async_trait::async_trait;
use rand::Rng;
use xraft_core::error::{Result, XraftError};
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;

/// Configuration for fault injection in `MemoryLogStore`.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Probability (0.0–1.0) that an append will fail with a simulated fsync error.
    pub fsync_failure_probability: f64,
    /// If true, injected writes will silently corrupt one entry per append.
    pub write_corruption: bool,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            fsync_failure_probability: 0.0,
            write_corruption: false,
        }
    }
}

/// In-memory log store backed by `Vec<LogEntry>`.
/// Uses interior mutability via `std::sync::Mutex` to satisfy
/// the `&self` + `Send + Sync` trait requirements.
pub struct MemoryLogStore {
    inner: Mutex<LogInner>,
    fault_config: Mutex<FaultConfig>,
}

struct LogInner {
    entries: Vec<LogEntry>,
    /// The offset of the first entry in `entries`.
    /// Advances when `truncate_prefix` is called.
    start_offset: u64,
}

impl MemoryLogStore {
    /// Create a new empty `MemoryLogStore`.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LogInner {
                entries: Vec::new(),
                start_offset: 0,
            }),
            fault_config: Mutex::new(FaultConfig::default()),
        }
    }

    /// Create a `MemoryLogStore` with fault injection enabled.
    pub fn with_fault_config(config: FaultConfig) -> Self {
        Self {
            inner: Mutex::new(LogInner {
                entries: Vec::new(),
                start_offset: 0,
            }),
            fault_config: Mutex::new(config),
        }
    }

    /// Update the fault injection configuration at runtime.
    pub fn set_fault_config(&self, config: FaultConfig) {
        *self.fault_config.lock().unwrap() = config;
    }

    fn should_fail_fsync(&self) -> bool {
        let config = self.fault_config.lock().unwrap();
        if config.fsync_failure_probability <= 0.0 {
            return false;
        }
        if config.fsync_failure_probability >= 1.0 {
            return true;
        }
        let mut rng = rand::thread_rng();
        rng.gen::<f64>() < config.fsync_failure_probability
    }

    fn should_corrupt(&self) -> bool {
        self.fault_config.lock().unwrap().write_corruption
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
        if self.should_fail_fsync() {
            return Err(XraftError::StorageError(
                "simulated fsync failure".to_string(),
            ));
        }

        let mut inner = self.inner.lock().unwrap();
        for entry in entries {
            let expected_offset = inner.start_offset + inner.entries.len() as u64;
            if entry.offset != expected_offset {
                return Err(XraftError::StorageError(format!(
                    "non-contiguous append: expected offset {expected_offset}, got {}",
                    entry.offset,
                )));
            }

            let mut entry = entry.clone();
            if self.should_corrupt() {
                let mut corrupted = entry.payload.to_vec();
                if !corrupted.is_empty() {
                    corrupted[0] ^= 0xFF;
                }
                entry.payload = bytes::Bytes::from(corrupted);
            }

            inner.entries.push(entry);
        }
        Ok(())
    }

    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>> {
        let inner = self.inner.lock().unwrap();

        if start_offset >= end_offset {
            return Ok(Vec::new());
        }
        if start_offset < inner.start_offset {
            return Err(XraftError::StorageError(format!(
                "start_offset {} is before log start {}",
                start_offset, inner.start_offset
            )));
        }

        let rel_start = (start_offset - inner.start_offset) as usize;
        let rel_end = (end_offset - inner.start_offset) as usize;
        let rel_end = rel_end.min(inner.entries.len());

        if rel_start >= inner.entries.len() {
            return Ok(Vec::new());
        }

        Ok(inner.entries[rel_start..rel_end].to_vec())
    }

    async fn truncate_suffix(&self, from_offset: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if from_offset < inner.start_offset {
            return Ok(());
        }
        let rel_offset = (from_offset - inner.start_offset) as usize;
        if rel_offset < inner.entries.len() {
            inner.entries.truncate(rel_offset);
        }
        Ok(())
    }

    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if up_to_offset <= inner.start_offset {
            return Ok(());
        }
        let to_remove = (up_to_offset - inner.start_offset) as usize;
        let to_remove = to_remove.min(inner.entries.len());
        inner.entries.drain(..to_remove);
        inner.start_offset = up_to_offset;
        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        self.inner.lock().unwrap().start_offset
    }

    fn log_end_offset(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.start_offset + inner.entries.len() as u64
    }

    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>> {
        let inner = self.inner.lock().unwrap();
        if offset < inner.start_offset {
            return Ok(None);
        }
        let rel = (offset - inner.start_offset) as usize;
        Ok(inner.entries.get(rel).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use xraft_core::log_entry::EntryType;

    fn make_entry(offset: u64, term: u64, data: &[u8]) -> LogEntry {
        LogEntry {
            offset,
            term,
            entry_type: EntryType::Command,
            payload: Bytes::copy_from_slice(data),
        }
    }

    #[tokio::test]
    async fn test_memory_log_store_append_and_read_1000_entries() {
        let store = MemoryLogStore::new();

        // Append 1000 entries
        let entries: Vec<LogEntry> = (0..1000)
            .map(|i| make_entry(i, 1, format!("data-{i}").as_bytes()))
            .collect();
        store.append(&entries).await.unwrap();

        assert_eq!(store.log_start_offset(), 0);
        assert_eq!(store.log_end_offset(), 1000);

        // Read all back
        let read_back = store.read(0, 1000).await.unwrap();
        assert_eq!(read_back.len(), 1000);

        for (i, entry) in read_back.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
            assert_eq!(entry.term, 1);
            assert_eq!(entry.entry_type, EntryType::Command);
            let expected_payload = format!("data-{i}");
            assert_eq!(entry.payload, Bytes::from(expected_payload));
        }
    }

    #[tokio::test]
    async fn test_memory_log_store_read_range() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..10)
            .map(|i| make_entry(i, 1, &[i as u8]))
            .collect();
        store.append(&entries).await.unwrap();

        let slice = store.read(3, 7).await.unwrap();
        assert_eq!(slice.len(), 4);
        assert_eq!(slice[0].offset, 3);
        assert_eq!(slice[3].offset, 6);
    }

    #[tokio::test]
    async fn test_memory_log_store_truncate_suffix() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..10)
            .map(|i| make_entry(i, 1, &[i as u8]))
            .collect();
        store.append(&entries).await.unwrap();

        store.truncate_suffix(5).await.unwrap();
        assert_eq!(store.log_end_offset(), 5);
        assert_eq!(store.log_start_offset(), 0);

        let remaining = store.read(0, 10).await.unwrap();
        assert_eq!(remaining.len(), 5);
    }

    #[tokio::test]
    async fn test_memory_log_store_truncate_prefix() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..10)
            .map(|i| make_entry(i, 1, &[i as u8]))
            .collect();
        store.append(&entries).await.unwrap();

        store.truncate_prefix(3).await.unwrap();
        assert_eq!(store.log_start_offset(), 3);
        assert_eq!(store.log_end_offset(), 10);

        let remaining = store.read(3, 10).await.unwrap();
        assert_eq!(remaining.len(), 7);
        assert_eq!(remaining[0].offset, 3);
    }

    #[tokio::test]
    async fn test_memory_log_store_entry_at() {
        let store = MemoryLogStore::new();
        let entries: Vec<LogEntry> = (0..5)
            .map(|i| make_entry(i, 2, &[i as u8]))
            .collect();
        store.append(&entries).await.unwrap();

        let entry = store.entry_at(3).await.unwrap().unwrap();
        assert_eq!(entry.offset, 3);
        assert_eq!(entry.term, 2);

        assert!(store.entry_at(10).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_fault_injection_fsync_failure() {
        let config = FaultConfig {
            fsync_failure_probability: 1.0,
            write_corruption: false,
        };
        let store = MemoryLogStore::with_fault_config(config);

        let entries = vec![make_entry(0, 1, b"hello")];
        let result = store.append(&entries).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::StorageError(msg) => {
                assert!(msg.contains("fsync"));
            }
            other => panic!("expected StorageError, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_fault_injection_write_corruption() {
        let config = FaultConfig {
            fsync_failure_probability: 0.0,
            write_corruption: true,
        };
        let store = MemoryLogStore::with_fault_config(config);

        let original_data = b"hello";
        let entries = vec![make_entry(0, 1, original_data)];
        store.append(&entries).await.unwrap();

        let read_back = store.entry_at(0).await.unwrap().unwrap();
        assert_ne!(read_back.payload, Bytes::from_static(original_data));
    }

    #[tokio::test]
    async fn test_offset_tracking_after_operations() {
        let store = MemoryLogStore::new();
        assert_eq!(store.log_start_offset(), 0);
        assert_eq!(store.log_end_offset(), 0);

        let entries: Vec<LogEntry> = (0..5)
            .map(|i| make_entry(i, 1, &[i as u8]))
            .collect();
        store.append(&entries).await.unwrap();
        assert_eq!(store.log_start_offset(), 0);
        assert_eq!(store.log_end_offset(), 5);

        store.truncate_prefix(2).await.unwrap();
        assert_eq!(store.log_start_offset(), 2);
        assert_eq!(store.log_end_offset(), 5);

        store.truncate_suffix(4).await.unwrap();
        assert_eq!(store.log_start_offset(), 2);
        assert_eq!(store.log_end_offset(), 4);
    }

    #[tokio::test]
    async fn test_append_rejects_non_contiguous_offsets() {
        let store = MemoryLogStore::new();

        // Append with offset gap: expected 0, given 5
        let entries = vec![make_entry(5, 1, b"bad")];
        let result = store.append(&entries).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::StorageError(msg) => {
                assert!(msg.contains("non-contiguous"));
            }
            other => panic!("expected StorageError, got: {other}"),
        }

        // Log should be unchanged
        assert_eq!(store.log_end_offset(), 0);
    }

    #[tokio::test]
    async fn test_append_rejects_gap_after_existing_entries() {
        let store = MemoryLogStore::new();

        let entries: Vec<LogEntry> = (0..3)
            .map(|i| make_entry(i, 1, &[i as u8]))
            .collect();
        store.append(&entries).await.unwrap();
        assert_eq!(store.log_end_offset(), 3);

        // Try to append with offset 10 instead of 3
        let bad_entries = vec![make_entry(10, 2, b"gap")];
        let result = store.append(&bad_entries).await;
        assert!(result.is_err());

        // Original entries untouched
        assert_eq!(store.log_end_offset(), 3);
    }
}
