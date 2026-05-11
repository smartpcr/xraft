use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// A single log entry in the Raft log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

/// On-disk format helpers.
/// Entry wire format: [4-byte big-endian payload length][payload]
/// Payload: [8-byte term][8-byte index][remaining data bytes]
const HEADER_LEN: usize = 4;
const TERM_LEN: usize = 8;
const INDEX_LEN: usize = 8;
const ENTRY_META_LEN: usize = TERM_LEN + INDEX_LEN;

/// A single segment file that stores a contiguous range of log entries.
///
/// The file handle is kept open for the lifetime of the `Segment` to avoid
/// reopening the file on every append (see review feedback on I/O overhead).
pub struct Segment {
    /// The first log index stored in this segment.
    pub base_index: u64,
    /// Number of entries currently in this segment.
    entry_count: u64,
    /// On-disk path.
    path: PathBuf,
    /// Cached writer — avoids reopening the file on every `append_entry` call.
    writer: BufWriter<File>,
}

impl Segment {
    /// Create a new, empty segment file starting at `base_index`.
    pub fn create(dir: &Path, base_index: u64) -> io::Result<Self> {
        let path = dir.join(format!("segment-{:020}.log", base_index));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Segment {
            base_index,
            entry_count: 0,
            path,
            writer: BufWriter::new(file),
        })
    }

    /// Open an existing segment file and rebuild in-memory state.
    /// Truncates any trailing partial record to recover from crashes.
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let base_index = parse_base_index(&path)?;

        // Scan all valid entries to find count and last valid offset.
        let (entry_count, valid_len) = scan_entries(&path)?;

        // Truncate trailing garbage (partial write from a crash).
        let file_len = fs::metadata(&path)?.len();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        if valid_len < file_len {
            file.set_len(valid_len)?;
            file.sync_data()?;
        }

        // Seek to end so subsequent writes append.
        file.seek(SeekFrom::End(0))?;

        Ok(Segment {
            base_index,
            entry_count,
            path,
            writer: BufWriter::new(file),
        })
    }

    /// Append a single entry. The caller is responsible for ensuring indices
    /// are contiguous.
    pub fn append_entry(&mut self, entry: &LogEntry) -> io::Result<()> {
        let payload_len = ENTRY_META_LEN + entry.data.len();
        self.writer
            .write_all(&(payload_len as u32).to_be_bytes())?;
        self.writer.write_all(&entry.term.to_be_bytes())?;
        self.writer.write_all(&entry.index.to_be_bytes())?;
        self.writer.write_all(&entry.data)?;
        self.entry_count += 1;
        Ok(())
    }

    /// Flush user-space buffers and fsync to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_mut().sync_data()
    }

    /// The next index that would be appended to this segment.
    pub fn next_index(&self) -> u64 {
        self.base_index + self.entry_count
    }

    /// Number of entries in this segment.
    pub fn len(&self) -> u64 {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Read all entries from this segment.
    pub fn read_entries(&mut self) -> io::Result<Vec<LogEntry>> {
        // Flush so any buffered appends are visible to the reader.
        self.writer.flush()?;

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            match read_one_entry(&mut reader) {
                Ok(entry) => entries.push(entry),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        Ok(entries)
    }

    /// Read a single entry by its absolute log index.
    pub fn read_entry(&mut self, index: u64) -> io::Result<Option<LogEntry>> {
        if index < self.base_index || index >= self.next_index() {
            return Ok(None);
        }
        // Flush buffered writes so the reader can see them.
        self.writer.flush()?;

        let target_offset = index - self.base_index;
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        for _ in 0..target_offset {
            skip_one_entry(&mut reader)?;
        }
        read_one_entry(&mut reader).map(Some)
    }

    /// Truncate this segment so that it retains only entries with index < `from_index`.
    /// Returns the number of entries removed.
    pub fn truncate_from(&mut self, from_index: u64) -> io::Result<u64> {
        if from_index >= self.next_index() {
            return Ok(0);
        }
        let keep = if from_index <= self.base_index {
            0
        } else {
            from_index - self.base_index
        };

        // Flush before truncating so buffered data lands on disk first.
        self.writer.flush()?;

        // Find the byte offset after the last kept entry.
        let new_len = if keep == 0 {
            0
        } else {
            let file = File::open(&self.path)?;
            let mut reader = BufReader::new(file);
            let mut offset: u64 = 0;
            for _ in 0..keep {
                let payload_len = read_u32_be(&mut reader)? as u64;
                let entry_len = HEADER_LEN as u64 + payload_len;
                reader.seek(SeekFrom::Current(payload_len as i64))?;
                offset += entry_len;
            }
            offset
        };

        let removed = self.entry_count - keep;
        let file = self.writer.get_mut();
        file.set_len(new_len)?;
        file.seek(SeekFrom::Start(new_len))?;
        file.sync_data()?;
        self.entry_count = keep;
        Ok(removed)
    }

    /// Path to the underlying segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        // Best-effort flush; callers should use `sync()` for durability.
        let _ = self.writer.flush();
    }
}

// ---------------------------------------------------------------------------
// SegmentLog — manages an ordered collection of Segments
// ---------------------------------------------------------------------------

/// A segmented append-only log composed of multiple `Segment` files.
pub struct SegmentLog {
    dir: PathBuf,
    segments: Vec<Segment>,
    /// Maximum entries per segment before rolling to a new one.
    max_segment_entries: u64,
}

impl SegmentLog {
    /// Open or create a segment log in `dir`.
    pub fn open(dir: &Path, max_segment_entries: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let mut seg_paths: Vec<PathBuf> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().map_or(false, |ext| ext == "log")
                    && p.file_name()
                        .map_or(false, |n| n.to_string_lossy().starts_with("segment-"))
            })
            .collect();
        seg_paths.sort();

        let mut segments = Vec::new();
        for p in seg_paths {
            segments.push(Segment::open(p)?);
        }

        Ok(SegmentLog {
            dir: dir.to_path_buf(),
            segments,
            max_segment_entries,
        })
    }

    /// Append a batch of entries to the log.
    pub fn append(&mut self, entries: &[LogEntry]) -> io::Result<()> {
        for entry in entries {
            // Roll to a new segment if the active one is full.
            if self.needs_roll() {
                let base = self.next_index();
                let seg = Segment::create(&self.dir, base)?;
                self.segments.push(seg);
            }
            let seg = self
                .segments
                .last_mut()
                .expect("segment must exist after roll check");
            seg.append_entry(entry)?;
        }
        // Flush the active segment after the batch.
        if let Some(seg) = self.segments.last_mut() {
            seg.sync()?;
        }
        Ok(())
    }

    /// Read a single entry by absolute log index.
    pub fn read_entry(&mut self, index: u64) -> io::Result<Option<LogEntry>> {
        let seg = match self.segment_for_index_mut(index) {
            Some(s) => s,
            None => return Ok(None),
        };
        seg.read_entry(index)
    }

    /// Read entries in the range `[from, to)`.
    pub fn read_entries(&mut self, from: u64, to: u64) -> io::Result<Vec<LogEntry>> {
        let mut result = Vec::new();
        for idx in from..to {
            match self.read_entry(idx)? {
                Some(e) => result.push(e),
                None => break,
            }
        }
        Ok(result)
    }

    /// Truncate all entries with index >= `from_index`.
    pub fn truncate_from(&mut self, from_index: u64) -> io::Result<()> {
        // Remove segments that are entirely past from_index.
        while let Some(seg) = self.segments.last() {
            if seg.base_index >= from_index {
                let path = seg.path().to_path_buf();
                self.segments.pop();
                // Drop the segment (closes handle) before removing file.
                fs::remove_file(&path)?;
            } else {
                break;
            }
        }
        // Truncate the remaining active segment if it partially overlaps.
        if let Some(seg) = self.segments.last_mut() {
            seg.truncate_from(from_index)?;
        }
        Ok(())
    }

    /// The next log index that would be appended.
    pub fn next_index(&self) -> u64 {
        self.segments
            .last()
            .map_or(0, |s| s.next_index())
    }

    /// The first log index in the log, or `None` if empty.
    pub fn first_index(&self) -> Option<u64> {
        self.segments.first().map(|s| s.base_index)
    }

    /// Sync all segments to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        for seg in &mut self.segments {
            seg.sync()?;
        }
        Ok(())
    }

    // -- private helpers --

    fn needs_roll(&self) -> bool {
        match self.segments.last() {
            None => true,
            Some(seg) => seg.len() >= self.max_segment_entries,
        }
    }

    fn segment_for_index_mut(&mut self, index: u64) -> Option<&mut Segment> {
        self.segments.iter_mut().rev().find(|s| index >= s.base_index && index < s.next_index())
    }
}

// ---------------------------------------------------------------------------
// Wire-format helpers
// ---------------------------------------------------------------------------

fn read_u32_be(r: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64_be(r: &mut impl Read) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

fn read_one_entry(r: &mut (impl Read + Seek)) -> io::Result<LogEntry> {
    let payload_len = read_u32_be(r)? as usize;
    if payload_len < ENTRY_META_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "entry payload too short",
        ));
    }
    let term = read_u64_be(r)?;
    let index = read_u64_be(r)?;
    let data_len = payload_len - ENTRY_META_LEN;
    let mut data = vec![0u8; data_len];
    r.read_exact(&mut data)?;
    Ok(LogEntry { term, index, data })
}

fn skip_one_entry(r: &mut (impl Read + Seek)) -> io::Result<()> {
    let payload_len = read_u32_be(r)? as u64;
    r.seek(SeekFrom::Current(payload_len as i64))?;
    Ok(())
}

/// Scan a segment file, returning (valid_entry_count, valid_byte_length).
/// Used on open to detect and recover from partial trailing writes.
fn scan_entries(path: &Path) -> io::Result<(u64, u64)> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut count: u64 = 0;
    let mut valid_end: u64 = 0;

    loop {
        if valid_end >= file_len {
            break;
        }
        let start = valid_end;
        match read_u32_be(&mut reader) {
            Ok(payload_len) => {
                let entry_end = start + HEADER_LEN as u64 + payload_len as u64;
                if entry_end > file_len {
                    // Partial record — stop here.
                    break;
                }
                if (payload_len as usize) < ENTRY_META_LEN {
                    break;
                }
                if let Err(_) = reader.seek(SeekFrom::Current(payload_len as i64)) {
                    break;
                }
                count += 1;
                valid_end = entry_end;
            }
            Err(_) => break,
        }
    }
    Ok((count, valid_end))
}

fn parse_base_index(path: &Path) -> io::Result<u64> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad segment filename"))?;
    let idx_str = stem
        .strip_prefix("segment-")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing segment- prefix"))?;
    idx_str
        .parse::<u64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("xraft-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_entry(term: u64, index: u64, data: &[u8]) -> LogEntry {
        LogEntry {
            term,
            index,
            data: data.to_vec(),
        }
    }

    #[test]
    fn test_segment_append_and_read() {
        let dir = tmp_dir();
        let mut seg = Segment::create(&dir, 1).unwrap();
        let e1 = make_entry(1, 1, b"hello");
        let e2 = make_entry(1, 2, b"world");
        seg.append_entry(&e1).unwrap();
        seg.append_entry(&e2).unwrap();
        seg.sync().unwrap();

        assert_eq!(seg.len(), 2);
        assert_eq!(seg.next_index(), 3);

        let entries = seg.read_entries().unwrap();
        assert_eq!(entries, vec![e1.clone(), e2.clone()]);

        assert_eq!(seg.read_entry(1).unwrap(), Some(e1));
        assert_eq!(seg.read_entry(2).unwrap(), Some(e2));
        assert_eq!(seg.read_entry(3).unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_segment_truncate() {
        let dir = tmp_dir();
        let mut seg = Segment::create(&dir, 1).unwrap();
        for i in 1..=5 {
            seg.append_entry(&make_entry(1, i, b"x")).unwrap();
        }
        seg.sync().unwrap();

        seg.truncate_from(3).unwrap();
        assert_eq!(seg.len(), 2);
        assert_eq!(seg.next_index(), 3);
        let entries = seg.read_entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 2);

        // Can append after truncation.
        seg.append_entry(&make_entry(2, 3, b"new")).unwrap();
        seg.sync().unwrap();
        assert_eq!(seg.len(), 3);
        let e = seg.read_entry(3).unwrap().unwrap();
        assert_eq!(e.term, 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_segment_log_batch_append() {
        let dir = tmp_dir();
        let mut log = SegmentLog::open(&dir, 3).unwrap();
        let entries: Vec<LogEntry> = (0..7)
            .map(|i| make_entry(1, i, format!("data-{}", i).as_bytes()))
            .collect();
        log.append(&entries).unwrap();

        assert_eq!(log.segments.len(), 3); // 3 + 3 + 1
        for i in 0..7u64 {
            let e = log.read_entry(i).unwrap().unwrap();
            assert_eq!(e.index, i);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_segment_log_truncate() {
        let dir = tmp_dir();
        let mut log = SegmentLog::open(&dir, 3).unwrap();
        let entries: Vec<LogEntry> = (0..7)
            .map(|i| make_entry(1, i, b"d"))
            .collect();
        log.append(&entries).unwrap();

        log.truncate_from(4).unwrap();
        assert_eq!(log.next_index(), 4);
        assert!(log.read_entry(4).unwrap().is_none());
        assert!(log.read_entry(3).unwrap().is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_segment_reopen_recovers() {
        let dir = tmp_dir();
        let path;
        {
            let mut seg = Segment::create(&dir, 10).unwrap();
            seg.append_entry(&make_entry(1, 10, b"a")).unwrap();
            seg.append_entry(&make_entry(1, 11, b"b")).unwrap();
            seg.sync().unwrap();
            path = seg.path().to_path_buf();
        }
        // Append some garbage bytes to simulate a partial write.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xFF, 0x00, 0x01]).unwrap();
        }

        let mut seg = Segment::open(path).unwrap();
        assert_eq!(seg.len(), 2);
        let entries = seg.read_entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 10);
        let _ = fs::remove_dir_all(&dir);
    }
}
