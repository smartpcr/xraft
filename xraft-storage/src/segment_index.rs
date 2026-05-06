use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Entry size in the index file: offset (u64) + byte position (u64) = 16 bytes.
const INDEX_ENTRY_SIZE: usize = 16;

/// Sparse index mapping log offsets to byte positions in the corresponding
/// `.log` file. Every Nth entry is recorded, enabling O(log n) lookups via
/// binary search.
pub struct SparseIndex {
    /// In-memory copy of index entries: (log_offset, byte_position).
    entries: Vec<(u64, u64)>,
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    /// Record an index entry every `interval` log entries.
    interval: u32,
}

impl SparseIndex {
    /// Create a brand-new, empty index file.
    pub fn create(path: &Path, interval: u32) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;

        Ok(Self {
            entries: Vec::new(),
            file,
            path: path.to_path_buf(),
            interval,
        })
    }

    /// Create or truncate an index file (used during recovery rebuild).
    pub fn create_or_truncate(path: &Path, interval: u32) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            entries: Vec::new(),
            file,
            path: path.to_path_buf(),
            interval,
        })
    }

    /// Load an existing index file into memory.
    #[allow(dead_code)]
    pub fn open(path: &Path, interval: u32) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let file_len = file.metadata()?.len();
        // Truncate any partial trailing entry
        let valid_len = file_len - (file_len % INDEX_ENTRY_SIZE as u64);
        if valid_len < file_len {
            file.set_len(valid_len)?;
        }

        file.seek(SeekFrom::Start(0))?;

        let entry_count = (valid_len / INDEX_ENTRY_SIZE as u64) as usize;
        let mut entries = Vec::with_capacity(entry_count);
        let mut buf = [0u8; INDEX_ENTRY_SIZE];

        for _ in 0..entry_count {
            file.read_exact(&mut buf)?;
            let offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let position = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            entries.push((offset, position));
        }

        Ok(Self {
            entries,
            file,
            path: path.to_path_buf(),
            interval,
        })
    }

    /// Append an index entry. Caller decides when to call this (every Nth entry).
    pub fn append(&mut self, offset: u64, byte_position: u64) -> io::Result<()> {
        self.entries.push((offset, byte_position));

        // Write to file
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&offset.to_le_bytes())?;
        self.file.write_all(&byte_position.to_le_bytes())?;

        Ok(())
    }

    /// Flush the index file to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Binary search for the largest indexed offset ≤ `target_offset`.
    /// Returns the byte position to start scanning from, or `None` if
    /// the index is empty or all entries are after `target_offset`.
    pub fn lookup(&self, target_offset: u64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search: find rightmost entry where offset <= target_offset
        let mut lo = 0usize;
        let mut hi = self.entries.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.entries[mid].0 <= target_offset {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if lo == 0 {
            None
        } else {
            Some(self.entries[lo - 1].1)
        }
    }

    /// The index recording interval.
    pub fn interval(&self) -> u32 {
        self.interval
    }

    /// Number of entries in the index.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn lookup_empty_returns_none() {
        let dir = TempDir::new().unwrap();
        let idx = SparseIndex::create(&dir.path().join("test.index"), 16).unwrap();
        assert_eq!(idx.lookup(0), None);
        assert_eq!(idx.lookup(100), None);
    }

    #[test]
    fn lookup_finds_floor_entry() {
        let dir = TempDir::new().unwrap();
        let mut idx = SparseIndex::create(&dir.path().join("test.index"), 16).unwrap();

        // Offsets: 0, 16, 32, 48
        idx.append(0, 0).unwrap();
        idx.append(16, 1000).unwrap();
        idx.append(32, 2500).unwrap();
        idx.append(48, 4000).unwrap();
        idx.flush().unwrap();

        assert_eq!(idx.lookup(0), Some(0));
        assert_eq!(idx.lookup(5), Some(0));
        assert_eq!(idx.lookup(16), Some(1000));
        assert_eq!(idx.lookup(31), Some(1000));
        assert_eq!(idx.lookup(32), Some(2500));
        assert_eq!(idx.lookup(100), Some(4000));
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.index");

        {
            let mut idx = SparseIndex::create(&path, 16).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(16, 500).unwrap();
            idx.append(32, 1200).unwrap();
            idx.flush().unwrap();
        }

        let idx = SparseIndex::open(&path, 16).unwrap();
        assert_eq!(idx.len(), 3);
        assert_eq!(idx.lookup(20), Some(500));
    }
}
