use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tracing::warn;
use xraft_core::LogEntry;

use crate::segment_index::SegmentIndex;

/// Record format in a .log file (one record per log entry):
///
/// ```text
/// [4 bytes: CRC-32C of (entry_len bytes + entry_data bytes)]
/// [4 bytes: entry_len (u32 LE) — length of the serialized LogEntry]
/// [entry_len bytes: bincode-serialized LogEntry]
/// ```
const RECORD_HEADER_SIZE: usize = 8; // 4 (CRC) + 4 (len)

/// A single segment file covering a contiguous range of log offsets.
pub struct Segment {
    /// Base offset — the first offset stored in this segment.
    base_offset: u64,
    /// Path to the `.log` file.
    log_path: PathBuf,
    /// The next offset to be written (one past the last entry in this segment).
    next_offset: u64,
    /// Current file size in bytes.
    file_size: u64,
    /// Sparse index for this segment.
    index: SegmentIndex,
    /// Maximum segment file size.
    max_bytes: u64,
}

impl Segment {
    /// Create a new, empty segment starting at `base_offset`.
    pub fn create(dir: &Path, base_offset: u64, max_bytes: u64, index_interval: u32) -> io::Result<Self> {
        let log_path = dir.join(Self::filename(base_offset, "log"));
        let index_path = dir.join(Self::filename(base_offset, "index"));

        // Create the empty log file.
        std::fs::File::create(&log_path)?;

        let index = SegmentIndex::new(index_path, index_interval);

        Ok(Self {
            base_offset,
            log_path,
            next_offset: base_offset,
            file_size: 0,
            index,
            max_bytes,
        })
    }

    /// Open an existing segment, scanning for valid records.
    /// Returns the segment with `next_offset` and `file_size` set
    /// from the scan. Truncates at first corruption.
    pub fn open(
        dir: &Path,
        base_offset: u64,
        max_bytes: u64,
        index_interval: u32,
    ) -> io::Result<Self> {
        let log_path = dir.join(Self::filename(base_offset, "log"));
        let index_path = dir.join(Self::filename(base_offset, "index"));

        let mut seg = Self {
            base_offset,
            log_path: log_path.clone(),
            next_offset: base_offset,
            file_size: 0,
            index: SegmentIndex::new(index_path, index_interval),
            max_bytes,
        };

        // Recovery scan: read all records, validate CRC, rebuild index.
        seg.recovery_scan()?;

        Ok(seg)
    }

    /// Recovery scan: walk forward through the segment, validate CRCs,
    /// truncate at first corruption, and rebuild the sparse index.
    fn recovery_scan(&mut self) -> io::Result<()> {
        let mut file = std::fs::File::open(&self.log_path)?;
        let file_len = file.metadata()?.len();

        let mut position: u64 = 0;
        let mut offset = self.base_offset;
        let mut index_entries: Vec<(u64, u64)> = Vec::new();

        loop {
            if position + RECORD_HEADER_SIZE as u64 > file_len {
                // Not enough bytes for even a header — partial write, truncate here.
                if position < file_len {
                    warn!(
                        segment = %self.log_path.display(),
                        position,
                        "truncating partial record header at end of segment"
                    );
                }
                break;
            }

            file.seek(SeekFrom::Start(position))?;

            // Read header.
            let mut header_buf = [0u8; RECORD_HEADER_SIZE];
            if file.read_exact(&mut header_buf).is_err() {
                break;
            }
            let stored_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
            let entry_len = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

            // Sanity check entry_len (guard against corrupt length causing huge alloc).
            if entry_len == 0 || entry_len > 128 * 1024 * 1024 {
                warn!(
                    segment = %self.log_path.display(),
                    position,
                    entry_len,
                    "invalid entry length, truncating"
                );
                break;
            }

            if position + RECORD_HEADER_SIZE as u64 + entry_len as u64 > file_len {
                warn!(
                    segment = %self.log_path.display(),
                    position,
                    entry_len,
                    "incomplete record, truncating"
                );
                break;
            }

            // Read entry data.
            let mut entry_data = vec![0u8; entry_len as usize];
            if file.read_exact(&mut entry_data).is_err() {
                break;
            }

            // Validate CRC: CRC covers (entry_len bytes ++ entry_data).
            let mut crc_payload = Vec::with_capacity(4 + entry_data.len());
            crc_payload.extend_from_slice(&entry_len.to_le_bytes());
            crc_payload.extend_from_slice(&entry_data);
            let computed_crc = crc32c::crc32c(&crc_payload);

            if computed_crc != stored_crc {
                warn!(
                    segment = %self.log_path.display(),
                    position,
                    stored_crc,
                    computed_crc,
                    "CRC mismatch, truncating at this record"
                );
                break;
            }

            // Deserialize to verify the entry is well-formed.
            match bincode::deserialize::<LogEntry>(&entry_data) {
                Ok(entry) => {
                    if entry.offset != offset {
                        warn!(
                            segment = %self.log_path.display(),
                            position,
                            expected_offset = offset,
                            actual_offset = entry.offset,
                            "offset mismatch in recovered entry, truncating"
                        );
                        break;
                    }
                    index_entries.push((offset, position));
                    let record_size = RECORD_HEADER_SIZE as u64 + entry_len as u64;
                    position += record_size;
                    offset += 1;
                }
                Err(e) => {
                    warn!(
                        segment = %self.log_path.display(),
                        position,
                        error = %e,
                        "failed to deserialize entry, truncating"
                    );
                    break;
                }
            }
        }

        // Truncate file at the last valid position.
        if position < file_len {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&self.log_path)?;
            file.set_len(position)?;
            file.sync_all()?;
        }

        self.next_offset = offset;
        self.file_size = position;

        // Rebuild sparse index.
        self.index.rebuild_from_entries(&index_entries)?;

        Ok(())
    }

    /// Append a single entry to this segment. Returns the byte position where
    /// the record starts.
    pub fn append_entry(&mut self, entry: &LogEntry) -> io::Result<u64> {
        let entry_data = bincode::serialize(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let entry_len = entry_data.len() as u32;

        // CRC covers (entry_len ++ entry_data).
        let mut crc_payload = Vec::with_capacity(4 + entry_data.len());
        crc_payload.extend_from_slice(&entry_len.to_le_bytes());
        crc_payload.extend_from_slice(&entry_data);
        let crc = crc32c::crc32c(&crc_payload);

        let record_start = self.file_size;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        file.write_all(&crc.to_le_bytes())?;
        file.write_all(&entry_len.to_le_bytes())?;
        file.write_all(&entry_data)?;

        let record_size = RECORD_HEADER_SIZE as u64 + entry_len as u64;

        // Update sparse index.
        self.index.maybe_add(entry.offset, record_start)?;

        self.file_size += record_size;
        self.next_offset = entry.offset + 1;

        Ok(record_start)
    }

    /// Fsync the log file.
    pub fn flush(&self) -> io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&self.log_path)?;
        file.sync_all()
    }

    /// Read all entries in the offset range `[start, end)` from this segment.
    /// Only returns entries whose offsets fall within this segment's range.
    pub fn read_range(&self, start_offset: u64, end_offset: u64) -> io::Result<Vec<LogEntry>> {
        let effective_start = start_offset.max(self.base_offset);
        let effective_end = end_offset.min(self.next_offset);

        if effective_start >= effective_end {
            return Ok(Vec::new());
        }

        let mut file = std::fs::File::open(&self.log_path)?;

        // Use the sparse index to find a starting position close to effective_start.
        let start_pos = match self.index.lookup(effective_start) {
            Some((indexed_offset, position)) => {
                // Scan forward from the indexed position.
                file.seek(SeekFrom::Start(position))?;
                // Skip records until we reach effective_start.
                let mut pos = position;
                let mut current_offset = indexed_offset;
                while current_offset < effective_start {
                    let record = Self::read_record_at(&mut file, pos)?;
                    match record {
                        Some((_, record_size)) => {
                            pos += record_size;
                            current_offset += 1;
                            file.seek(SeekFrom::Start(pos))?;
                        }
                        None => return Ok(Vec::new()),
                    }
                }
                pos
            }
            None => {
                // No index entry; scan from the beginning.
                let mut pos = 0u64;
                let mut current_offset = self.base_offset;
                file.seek(SeekFrom::Start(0))?;
                while current_offset < effective_start {
                    let record = Self::read_record_at(&mut file, pos)?;
                    match record {
                        Some((_, record_size)) => {
                            pos += record_size;
                            current_offset += 1;
                            file.seek(SeekFrom::Start(pos))?;
                        }
                        None => return Ok(Vec::new()),
                    }
                }
                pos
            }
        };

        // Now read entries from effective_start to effective_end.
        let mut entries = Vec::new();
        let mut pos = start_pos;
        file.seek(SeekFrom::Start(pos))?;

        for _ in effective_start..effective_end {
            let record = Self::read_record_at(&mut file, pos)?;
            match record {
                Some((entry, record_size)) => {
                    entries.push(entry);
                    pos += record_size;
                    file.seek(SeekFrom::Start(pos))?;
                }
                None => break,
            }
        }

        Ok(entries)
    }

    /// Read a single record at the given file position.
    /// Returns `(LogEntry, record_total_size)` or `None` if at EOF / corrupt.
    fn read_record_at(file: &mut std::fs::File, _position: u64) -> io::Result<Option<(LogEntry, u64)>> {
        let mut header_buf = [0u8; RECORD_HEADER_SIZE];
        match file.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let stored_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let entry_len = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

        if entry_len == 0 || entry_len > 128 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid entry length: {entry_len}"),
            ));
        }

        let mut entry_data = vec![0u8; entry_len as usize];
        file.read_exact(&mut entry_data)?;

        // Validate CRC.
        let mut crc_payload = Vec::with_capacity(4 + entry_data.len());
        crc_payload.extend_from_slice(&entry_len.to_le_bytes());
        crc_payload.extend_from_slice(&entry_data);
        let computed_crc = crc32c::crc32c(&crc_payload);

        if computed_crc != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "CRC mismatch: stored={stored_crc:#x}, computed={computed_crc:#x}"
                ),
            ));
        }

        let entry: LogEntry = bincode::deserialize(&entry_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let record_size = RECORD_HEADER_SIZE as u64 + entry_len as u64;
        Ok(Some((entry, record_size)))
    }

    /// Find the byte position where the record for `target_offset` begins.
    /// Scans from the nearest index entry.
    pub fn find_position(&self, target_offset: u64) -> io::Result<Option<u64>> {
        if target_offset < self.base_offset || target_offset >= self.next_offset {
            return Ok(None);
        }

        let mut file = std::fs::File::open(&self.log_path)?;

        let (start_scan_offset, start_pos) = match self.index.lookup(target_offset) {
            Some((indexed_offset, position)) => (indexed_offset, position),
            None => (self.base_offset, 0),
        };

        let mut pos = start_pos;
        let mut current_offset = start_scan_offset;
        file.seek(SeekFrom::Start(pos))?;

        while current_offset < target_offset {
            let record = Self::read_record_at(&mut file, pos)?;
            match record {
                Some((_, record_size)) => {
                    pos += record_size;
                    current_offset += 1;
                    file.seek(SeekFrom::Start(pos))?;
                }
                None => return Ok(None),
            }
        }

        Ok(Some(pos))
    }

    /// Truncate all records at and after `from_offset`.
    pub fn truncate_at(&mut self, from_offset: u64) -> io::Result<()> {
        if from_offset >= self.next_offset {
            return Ok(());
        }

        if from_offset <= self.base_offset {
            // Truncate entire segment content.
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&self.log_path)?;
            file.set_len(0)?;
            file.sync_all()?;
            self.next_offset = self.base_offset;
            self.file_size = 0;
            self.index.truncate_from(self.base_offset)?;
            return Ok(());
        }

        // Find byte position of from_offset.
        let pos = self.find_position(from_offset)?;
        match pos {
            Some(truncate_pos) => {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&self.log_path)?;
                file.set_len(truncate_pos)?;
                file.sync_all()?;
                self.file_size = truncate_pos;
                self.next_offset = from_offset;
                self.index.truncate_from(from_offset)?;
            }
            None => {
                // This shouldn't happen for valid from_offset in range.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("could not find position for offset {from_offset}"),
                ));
            }
        }

        Ok(())
    }

    /// Remove this segment's log and index files from disk.
    pub fn remove(self) -> io::Result<()> {
        if self.log_path.exists() {
            remove_file_with_retry(&self.log_path)?;
        }
        self.index.remove()?;
        Ok(())
    }

    /// Zero-padded filename for a segment file.
    pub fn filename(base_offset: u64, ext: &str) -> String {
        format!("{:020}.{ext}", base_offset)
    }

    // Accessors

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn is_full(&self) -> bool {
        self.file_size >= self.max_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.next_offset == self.base_offset
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }
}

/// Remove a file with retries on Windows to handle delayed handle release
/// (antivirus scanners, search indexer, lazy close, etc.).
fn remove_file_with_retry(path: &Path) -> io::Result<()> {
    let max_attempts = 20;
    let mut last_err = None;
    for attempt in 0..max_attempts {
        match std::fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied && attempt < max_attempts - 1 => {
                last_err = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap())
}
