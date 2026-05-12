use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use xraft_core::log_entry::LogEntry;

use crate::segment_index::SparseIndex;

/// On-disk batch layout:
///   batch_len:    u32 — byte count after this field (crc + payload)
///   crc32c:       u32 — checksum over payload (num_entries + entry frames)
///   num_entries:  u32 — number of entries in this batch
///   entry frames: [entry_len: u32, entry_data: [u8]] × num_entries
///
/// CRC covers the entire payload (num_entries field + all entry frames),
/// providing per-batch integrity validation.
const BATCH_HEADER_LEN: u64 = 4 + 4 + 4; // batch_len + crc + num_entries
const MAX_BATCH_DATA: u32 = 64 * 1024 * 1024; // 64 MiB sanity cap

/// Formats a base offset into the canonical zero-padded 20-digit filename.
pub fn segment_filename(base_offset: u64) -> String {
    format!("{:020}", base_offset)
}

/// A pre-serialized log entry ready for batch writing.
pub struct SerializedEntry {
    /// bincode-serialized bytes of the LogEntry.
    pub data: Vec<u8>,
}

impl SerializedEntry {
    /// Serialize a LogEntry to its on-disk representation.
    pub fn from_entry(entry: &LogEntry) -> io::Result<Self> {
        let data = bincode::serialize(entry).map_err(io::Error::other)?;
        Ok(Self { data })
    }

    /// Size of one entry frame on disk: entry_len (u32) + data bytes.
    pub fn frame_size(&self) -> u64 {
        4 + self.data.len() as u64
    }
}

/// Total on-disk bytes for a batch of serialized entries.
pub fn batch_disk_size(entries: &[SerializedEntry]) -> u64 {
    // batch_len(4) + crc(4) + num_entries(4) + Σ(entry_len(4) + data)
    let frames: u64 = entries.iter().map(|e| e.frame_size()).sum();
    12 + frames
}

/// A single append-only log segment backed by a `.log` file on disk.
pub struct Segment {
    base_offset: u64,
    next_offset: u64,
    file_size: u64,
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    index: SparseIndex,
}

impl Segment {
    /// Create a brand-new, empty segment.
    pub fn create(dir: &Path, base_offset: u64, index_interval: u32) -> io::Result<Self> {
        let stem = segment_filename(base_offset);
        let log_path = dir.join(format!("{stem}.log"));
        let idx_path = dir.join(format!("{stem}.index"));

        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&log_path)?;

        let index = SparseIndex::create(&idx_path, index_interval)?;

        Ok(Self {
            base_offset,
            next_offset: base_offset,
            file_size: 0,
            file,
            path: log_path,
            index,
        })
    }

    /// Open an existing segment, scanning the log to rebuild in-memory state.
    pub fn open(dir: &Path, base_offset: u64, index_interval: u32) -> io::Result<Self> {
        let stem = segment_filename(base_offset);
        let log_path = dir.join(format!("{stem}.log"));
        let idx_path = dir.join(format!("{stem}.index"));

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)?;

        let (next_offset, file_size, index) =
            Self::recover_scan(&mut file, base_offset, &idx_path, index_interval)?;

        Ok(Self {
            base_offset,
            next_offset,
            file_size,
            file,
            path: log_path,
            index,
        })
    }

    /// Scan forward batch-by-batch, validating CRCs.
    /// Truncates any trailing partial/corrupt batches (recovery).
    fn recover_scan(
        file: &mut File,
        base_offset: u64,
        idx_path: &Path,
        index_interval: u32,
    ) -> io::Result<(u64, u64, SparseIndex)> {
        let total_len = file.metadata()?.len();
        file.seek(SeekFrom::Start(0))?;

        let mut index = SparseIndex::create_or_truncate(idx_path, index_interval)?;
        let mut pos: u64 = 0;
        let mut next_offset = base_offset;

        loop {
            if total_len.saturating_sub(pos) < BATCH_HEADER_LEN {
                break;
            }

            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let batch_len = u32::from_le_bytes(len_buf);

            // batch_len must cover at least crc(4) + num_entries(4) = 8
            if !(8..=MAX_BATCH_DATA).contains(&batch_len) {
                break;
            }
            if total_len.saturating_sub(pos + 4) < u64::from(batch_len) {
                break;
            }

            let mut batch_data = vec![0u8; batch_len as usize];
            if file.read_exact(&mut batch_data).is_err() {
                break;
            }

            // Validate CRC over payload (everything after the crc field)
            let stored_crc = u32::from_le_bytes([
                batch_data[0], batch_data[1], batch_data[2], batch_data[3],
            ]);
            let payload = &batch_data[4..];
            let computed_crc = crc32c::crc32c(payload);
            if stored_crc != computed_crc {
                break;
            }

            // Parse num_entries
            if payload.len() < 4 {
                break;
            }
            let num_entries = u32::from_le_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]);

            // Walk entry frames to validate deserialization
            let mut entry_pos = 4usize;
            let mut batch_valid = true;
            for _ in 0..num_entries {
                if entry_pos + 4 > payload.len() {
                    batch_valid = false;
                    break;
                }
                let entry_len = u32::from_le_bytes([
                    payload[entry_pos],
                    payload[entry_pos + 1],
                    payload[entry_pos + 2],
                    payload[entry_pos + 3],
                ]) as usize;
                entry_pos += 4;
                if entry_pos + entry_len > payload.len() {
                    batch_valid = false;
                    break;
                }
                let _: LogEntry = match bincode::deserialize(
                    &payload[entry_pos..entry_pos + entry_len],
                ) {
                    Ok(e) => e,
                    Err(_) => {
                        batch_valid = false;
                        break;
                    }
                };
                entry_pos += entry_len;
            }
            if !batch_valid {
                break;
            }

            // Record sparse index entries for this batch
            let batch_byte_pos = pos;
            let interval = u64::from(index_interval);
            for i in 0..u64::from(num_entries) {
                let global_idx = (next_offset + i) - base_offset;
                if global_idx.is_multiple_of(interval) {
                    index.append(next_offset + i, batch_byte_pos)?;
                }
            }

            next_offset += u64::from(num_entries);
            pos += 4 + u64::from(batch_len);
        }

        if pos < total_len {
            file.set_len(pos)?;
        }

        index.flush()?;
        Ok((next_offset, pos, index))
    }

    /// Write pre-serialized entries as a single CRC-protected batch.
    pub fn append_batch(
        &mut self,
        entries: &[LogEntry],
        serialized: &[SerializedEntry],
    ) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        assert_eq!(entries.len(), serialized.len());

        self.file.seek(SeekFrom::Start(self.file_size))?;

        // Build payload: num_entries + entry frames
        let num_entries = entries.len() as u32;
        let mut payload = Vec::new();
        payload.extend_from_slice(&num_entries.to_le_bytes());
        for se in serialized {
            let entry_len = se.data.len() as u32;
            payload.extend_from_slice(&entry_len.to_le_bytes());
            payload.extend_from_slice(&se.data);
        }

        let crc = crc32c::crc32c(&payload);
        let batch_len = (4 + payload.len()) as u32; // crc + payload

        // Record sparse index entries
        let batch_byte_pos = self.file_size;
        let interval = u64::from(self.index.interval());
        for i in 0..entries.len() {
            let global_idx = (self.next_offset + i as u64) - self.base_offset;
            if global_idx.is_multiple_of(interval) {
                self.index
                    .append(self.next_offset + i as u64, batch_byte_pos)?;
            }
        }

        // Write batch: batch_len + crc + payload
        self.file.write_all(&batch_len.to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;

        let written = 4 + u64::from(batch_len);
        self.file_size += written;
        self.next_offset += entries.len() as u64;

        self.file.sync_all()?;
        self.index.flush()?;

        Ok(())
    }

    /// Convenience: serialize entries internally and write as a single batch.
    pub fn append(&mut self, entries: &[LogEntry]) -> io::Result<()> {
        let serialized: Vec<SerializedEntry> = entries
            .iter()
            .map(SerializedEntry::from_entry)
            .collect::<io::Result<_>>()?;
        self.append_batch(entries, &serialized)
    }

    /// Read entries in `[start_offset, end_offset)` from this segment.
    /// Returns `Err` with `InvalidData` if a CRC mismatch is encountered
    /// in any batch that must be read.
    pub fn read(&mut self, start_offset: u64, end_offset: u64) -> io::Result<Vec<LogEntry>> {
        if start_offset >= end_offset || start_offset >= self.next_offset {
            return Ok(Vec::new());
        }

        let effective_end = end_offset.min(self.next_offset);

        let scan_pos = self.index.lookup(start_offset).unwrap_or(0);
        self.file.seek(SeekFrom::Start(scan_pos))?;

        let mut result = Vec::new();
        let mut pos = scan_pos;

        while pos < self.file_size {
            let batch_start = pos;

            // Read batch_len
            let mut len_buf = [0u8; 4];
            if let Err(e) = self.file.read_exact(&mut len_buf) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e);
            }
            let batch_len = u32::from_le_bytes(len_buf);

            if !(8..=MAX_BATCH_DATA).contains(&batch_len) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "corrupt batch at byte {batch_start}: invalid batch_len {batch_len}"
                    ),
                ));
            }

            let mut batch_data = vec![0u8; batch_len as usize];
            self.file.read_exact(&mut batch_data)?;

            // Validate batch CRC
            let stored_crc = u32::from_le_bytes([
                batch_data[0], batch_data[1], batch_data[2], batch_data[3],
            ]);
            let payload = &batch_data[4..];
            let computed_crc = crc32c::crc32c(payload);
            if stored_crc != computed_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "CRC mismatch at batch byte {batch_start}: \
                         stored={stored_crc:#010x} computed={computed_crc:#010x}"
                    ),
                ));
            }

            // Parse entries from payload
            if payload.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("truncated batch at byte {batch_start}"),
                ));
            }
            let num_entries = u32::from_le_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]);

            let mut entry_pos = 4usize;
            let mut found_past_end = false;
            for _ in 0..num_entries {
                if entry_pos + 4 > payload.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("truncated entry frame at byte {batch_start}"),
                    ));
                }
                let entry_len = u32::from_le_bytes([
                    payload[entry_pos],
                    payload[entry_pos + 1],
                    payload[entry_pos + 2],
                    payload[entry_pos + 3],
                ]) as usize;
                entry_pos += 4;
                if entry_pos + entry_len > payload.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("truncated entry data at byte {batch_start}"),
                    ));
                }
                let entry: LogEntry =
                    bincode::deserialize(&payload[entry_pos..entry_pos + entry_len])
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "deserialization failed at byte {batch_start}: {e}"
                                ),
                            )
                        })?;
                entry_pos += entry_len;

                if entry.offset >= effective_end {
                    found_past_end = true;
                    break;
                }
                if entry.offset >= start_offset {
                    result.push(entry);
                }
            }

            if found_past_end {
                break;
            }

            pos += 4 + u64::from(batch_len);
        }

        Ok(result)
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use xraft_core::log_entry::EntryType;
    use xraft_core::types::Term;

    fn make_entry(offset: u64, term: u64, payload: &[u8]) -> LogEntry {
        LogEntry {
            offset,
            term: Term(term),
            entry_type: EntryType::Command,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn append_and_read_back() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 0, 4).unwrap();

        let entries: Vec<LogEntry> = (0..10)
            .map(|i| make_entry(i, 1, &[i as u8; 8]))
            .collect();
        seg.append(&entries).unwrap();

        assert_eq!(seg.next_offset(), 10);
        assert!(seg.file_size() > 0);

        let read_back = seg.read(0, 10).unwrap();
        assert_eq!(read_back.len(), 10);
        for (i, entry) in read_back.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
            assert_eq!(entry.term, Term(1));
            assert_eq!(entry.payload, vec![i as u8; 8]);
        }
    }

    #[test]
    fn read_past_corruption_returns_storage_error() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 0, 4).unwrap();
        // Write 10 entries as individual batches
        for i in 0..10u64 {
            seg.append(&[make_entry(i, 1, &[i as u8; 16])]).unwrap();
        }

        // Corrupt a batch mid-file on disk (without reopening)
        let log_path = dir.path().join("00000000000000000000.log");
        let mut raw = std::fs::read(&log_path).unwrap();
        // Each batch has the same size; corrupt batch 5's payload area
        let batch_size = raw.len() / 10;
        let corrupt_byte = batch_size * 5 + 14; // inside entry data
        raw[corrupt_byte] ^= 0xFF;
        std::fs::write(&log_path, &raw).unwrap();

        // Read spanning the corruption → must return InvalidData error
        let err = seg.read(0, 10).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("CRC mismatch"),
            "expected CRC mismatch error, got: {err}"
        );
    }

    #[test]
    fn open_recovers_valid_segment() {
        let dir = TempDir::new().unwrap();

        {
            let mut seg = Segment::create(dir.path(), 0, 4).unwrap();
            let entries: Vec<LogEntry> = (0..20)
                .map(|i| make_entry(i, 1, &[i as u8; 8]))
                .collect();
            seg.append(&entries).unwrap();
        }

        let mut seg = Segment::open(dir.path(), 0, 4).unwrap();
        assert_eq!(seg.next_offset(), 20);

        let read_back = seg.read(0, 20).unwrap();
        assert_eq!(read_back.len(), 20);
    }

    #[test]
    fn recovery_truncates_corrupt_trailing_batch() {
        let dir = TempDir::new().unwrap();

        {
            let mut seg = Segment::create(dir.path(), 0, 4).unwrap();
            for i in 0..10u64 {
                seg.append(&[make_entry(i, 1, &[i as u8; 16])]).unwrap();
            }
        }

        // Corrupt the last batch
        let log_path = dir.path().join("00000000000000000000.log");
        let mut raw = std::fs::read(&log_path).unwrap();
        let batch_size = raw.len() / 10;
        let corrupt_byte = batch_size * 9 + 14;
        raw[corrupt_byte] ^= 0xFF;
        std::fs::write(&log_path, &raw).unwrap();

        // Recovery truncates from the corrupt batch onward
        let mut seg = Segment::open(dir.path(), 0, 4).unwrap();
        assert_eq!(seg.next_offset(), 9);
        let entries = seg.read(0, 9).unwrap();
        assert_eq!(entries.len(), 9);
    }

    #[test]
    fn sparse_index_read_at_interval_boundary() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 0, 4).unwrap();

        for i in 0..16u64 {
            seg.append(&[make_entry(i, 1, &[i as u8; 8])]).unwrap();
        }

        let read_at_4 = seg.read(4, 5).unwrap();
        assert_eq!(read_at_4.len(), 1);
        assert_eq!(read_at_4[0].offset, 4);

        let read_at_12 = seg.read(12, 13).unwrap();
        assert_eq!(read_at_12.len(), 1);
        assert_eq!(read_at_12[0].offset, 12);
    }
}
