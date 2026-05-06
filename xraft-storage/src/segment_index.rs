use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Sparse index for a single segment file.
///
/// Maps `offset → byte_position` in the `.log` file for every Nth entry.
/// The index file is disposable — it can be rebuilt from the log.
///
/// Format: repeated fixed-size entries of `[u64 offset][u64 position]` (16 bytes each).
pub struct SegmentIndex {
    path: PathBuf,
    /// Sorted (offset, file_position) pairs loaded in memory.
    entries: Vec<(u64, u64)>,
    /// Write an index entry every `interval` records.
    interval: u32,
    /// Counter: how many records since the last index entry.
    records_since_last: u32,
}

const _INDEX_ENTRY_SIZE: usize = 16; // 8 bytes offset + 8 bytes position

impl SegmentIndex {
    /// Create a new (empty) index.
    pub fn new(path: PathBuf, interval: u32) -> Self {
        Self {
            path,
            entries: Vec::new(),
            interval,
            records_since_last: 0,
        }
    }

    /// Open an existing index file. If it's corrupt or missing, return an empty index.
    pub fn open(path: PathBuf, interval: u32) -> Self {
        let entries = match std::fs::read(&path) {
            Ok(data) => Self::parse_entries(&data),
            Err(_) => Vec::new(),
        };
        // records_since_last is unknown after open; will be recalibrated on rebuild.
        Self {
            path,
            entries,
            interval,
            records_since_last: 0,
        }
    }

    fn parse_entries(data: &[u8]) -> Vec<(u64, u64)> {
        let mut entries = Vec::new();
        let mut cursor = std::io::Cursor::new(data);
        loop {
            let mut buf = [0u8; 8];
            if cursor.read_exact(&mut buf).is_err() {
                break;
            }
            let offset = u64::from_le_bytes(buf);
            if cursor.read_exact(&mut buf).is_err() {
                break;
            }
            let position = u64::from_le_bytes(buf);
            entries.push((offset, position));
        }
        entries
    }

    /// Look up the byte position nearest to (but not after) `target_offset`.
    /// Returns `(indexed_offset, file_position)` or `None` if index is empty.
    pub fn lookup(&self, target_offset: u64) -> Option<(u64, u64)> {
        if self.entries.is_empty() {
            return None;
        }
        // Binary search for the largest offset <= target_offset.
        let idx = self
            .entries
            .partition_point(|(off, _)| *off <= target_offset);
        if idx == 0 {
            None
        } else {
            Some(self.entries[idx - 1])
        }
    }

    /// Record that an entry was written at `offset` at byte `position`.
    /// Only actually writes to the index file every `interval` records.
    pub fn maybe_add(&mut self, offset: u64, position: u64) -> io::Result<()> {
        if self.records_since_last == 0 || self.records_since_last >= self.interval {
            self.entries.push((offset, position));
            self.append_to_file(offset, position)?;
            self.records_since_last = 1;
        } else {
            self.records_since_last += 1;
        }
        Ok(())
    }

    fn append_to_file(&self, offset: u64, position: u64) -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&offset.to_le_bytes())?;
        file.write_all(&position.to_le_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Truncate all index entries at or after `from_offset` and rewrite the file.
    pub fn truncate_from(&mut self, from_offset: u64) -> io::Result<()> {
        let keep = self
            .entries
            .partition_point(|(off, _)| *off < from_offset);
        self.entries.truncate(keep);
        self.rewrite_file()?;
        self.records_since_last = 0;
        Ok(())
    }

    /// Rebuild the index from the log data. Called during recovery.
    pub fn rebuild_from_entries(&mut self, entries: &[(u64, u64)]) -> io::Result<()> {
        self.entries.clear();
        self.records_since_last = 0;
        for &(offset, position) in entries {
            if self.records_since_last == 0 || self.records_since_last >= self.interval {
                self.entries.push((offset, position));
                self.records_since_last = 1;
            } else {
                self.records_since_last += 1;
            }
        }
        self.rewrite_file()
    }

    fn rewrite_file(&self) -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        for &(offset, position) in &self.entries {
            file.write_all(&offset.to_le_bytes())?;
            file.write_all(&position.to_le_bytes())?;
        }
        file.flush()?;
        Ok(())
    }

    /// The path to the index file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of entries in the index.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Remove the index file from disk.
    pub fn remove(self) -> io::Result<()> {
        if self.path.exists() {
            let max_attempts: u64 = 20;
            let mut last_err = None;
            for attempt in 0..max_attempts {
                match std::fs::remove_file(&self.path) {
                    Ok(()) => return Ok(()),
                    Err(e) if e.kind() == io::ErrorKind::PermissionDenied && attempt < max_attempts - 1 => {
                        last_err = Some(e);
                        std::thread::sleep(std::time::Duration::from_millis(100 * (attempt + 1)));
                    }
                    Err(e) => return Err(e),
                }
            }
            return Err(last_err.unwrap());
        }
        Ok(())
    }
}
