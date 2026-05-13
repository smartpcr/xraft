use xraft_core::error::{Result, XraftError};
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;
use xraft_core::types::Offset;

/// In-memory [`LogStore`] used for tests and simulation. Entries are kept
/// in a `Vec` indexed by `offset - base_offset` where `base_offset`
/// advances on `truncate_prefix` (e.g. after a snapshot install).
#[derive(Debug, Default)]
pub struct MemoryLogStore {
    entries: Vec<LogEntry>,
    base_offset: u64,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_base_offset(base_offset: u64) -> Self {
        Self {
            entries: Vec::new(),
            base_offset,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl LogStore for MemoryLogStore {
    fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let expected_next = match self.entries.last() {
            Some(last) => last.offset.0 + 1,
            None => self.base_offset,
        };
        if entries[0].offset.0 != expected_next {
            return Err(XraftError::StorageError(format!(
                "non-contiguous append: expected offset {}, got {}",
                expected_next, entries[0].offset.0
            )));
        }
        for window in entries.windows(2) {
            if window[1].offset.0 != window[0].offset.0 + 1 {
                return Err(XraftError::StorageError(format!(
                    "non-contiguous entries within batch: {} then {}",
                    window[0].offset.0, window[1].offset.0
                )));
            }
        }
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn read(&self, offset: Offset) -> Result<Option<LogEntry>> {
        if offset.0 < self.base_offset {
            return Ok(None);
        }
        let idx = (offset.0 - self.base_offset) as usize;
        Ok(self.entries.get(idx).cloned())
    }

    fn last_offset(&self) -> Result<Option<Offset>> {
        Ok(self.entries.last().map(|e| e.offset))
    }

    fn truncate_suffix(&mut self, from_offset: Offset) -> Result<()> {
        if from_offset.0 <= self.base_offset {
            self.entries.clear();
            return Ok(());
        }
        let idx = (from_offset.0 - self.base_offset) as usize;
        if idx < self.entries.len() {
            self.entries.truncate(idx);
        }
        Ok(())
    }

    fn truncate_prefix(&mut self, up_to_offset: Offset) -> Result<()> {
        if up_to_offset.0 <= self.base_offset {
            return Ok(());
        }
        let remove = ((up_to_offset.0 - self.base_offset) as usize).min(self.entries.len());
        self.entries.drain(..remove);
        self.base_offset = up_to_offset.0;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        // No-op for in-memory storage: there is nothing to fsync.
        // Provided for trait completeness; a disk-backed impl would
        // surface fsync failures as XraftError::StorageError.
        Ok(())
    }
}
