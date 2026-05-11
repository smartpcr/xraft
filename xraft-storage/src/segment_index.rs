use std::io::{self, Read, Seek, SeekFrom};

/// Header length (4-byte big-endian payload size) used by the segment wire format.
const HEADER_LEN: u64 = 4;
/// Term + index metadata bytes inside each entry payload.
const ENTRY_META_LEN: u64 = 16;

/// In-memory sparse index that maps selected log offsets to their byte
/// positions in the segment file, enabling O(log n) lookups instead of
/// O(n) linear scans.
///
/// Every `interval`-th entry appended to the segment is recorded.  On a
/// point read the caller binary-searches for the greatest indexed offset
/// ≤ the target, seeks to that byte position, then scans forward at most
/// `interval - 1` entries.
///
/// The index is rebuilt from the segment file on [`SparseIndex::rebuild`]
/// (called during `Segment::open`) and maintained incrementally on
/// [`SparseIndex::record_if_due`] (called during `Segment::append_entry`).
#[derive(Debug)]
pub struct SparseIndex {
    /// Sorted vec of (log_offset_within_segment, byte_position).
    entries: Vec<(u64, u64)>,
    /// Record every N-th entry.
    interval: u32,
    /// Entries appended since the last recorded index point.
    since_last: u32,
}

impl SparseIndex {
    /// Create an empty index that records every `interval` entries.
    ///
    /// An `interval` of 1 degenerates into a dense index (every entry
    /// indexed). Typical production value: 16.
    pub fn new(interval: u32) -> Self {
        assert!(interval > 0, "index interval must be ≥ 1");
        Self {
            entries: Vec::new(),
            interval,
            since_last: 0,
        }
    }

    /// Called on every `Segment::append_entry`.  Records the mapping when
    /// the entry is on an interval boundary.
    ///
    /// * `entry_seq` — zero-based sequence number within the segment
    ///   (i.e. `log_index - base_offset`).
    /// * `byte_pos` — byte offset in the segment file where this entry's
    ///   length header starts.
    pub fn record_if_due(&mut self, entry_seq: u64, byte_pos: u64) {
        if self.since_last == 0 {
            self.entries.push((entry_seq, byte_pos));
        }
        self.since_last += 1;
        if self.since_last >= self.interval {
            self.since_last = 0;
        }
    }

    /// Return the byte position of the nearest indexed entry at or before
    /// `entry_seq`, plus that entry's sequence number.  The caller scans
    /// forward from there.
    ///
    /// Returns `None` only when the index is empty (segment has no entries).
    pub fn floor(&self, entry_seq: u64) -> Option<(u64, u64)> {
        if self.entries.is_empty() {
            return None;
        }
        // Binary search: find the rightmost entry whose seq ≤ entry_seq.
        let idx = match self.entries.binary_search_by_key(&entry_seq, |&(seq, _)| seq) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };
        let (seq, pos) = self.entries[idx];
        Some((seq, pos))
    }

    /// Remove all index entries with sequence number ≥ `from_seq`.
    /// Called by `Segment::truncate_from` to keep the index consistent.
    pub fn truncate_from(&mut self, from_seq: u64) {
        let keep = match self.entries.binary_search_by_key(&from_seq, |&(seq, _)| seq) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.entries.truncate(keep);
        // Reset the modular counter so the next append records correctly.
        let last_seq = self.entries.last().map_or(0, |&(s, _)| s + 1);
        let entries_since = from_seq.saturating_sub(last_seq);
        self.since_last = (entries_since as u32) % self.interval;
    }

    /// Rebuild the index by scanning the segment file from byte 0.
    /// Used during `Segment::open` to restore the index from an existing
    /// segment without requiring a separate on-disk index file.
    ///
    /// Returns the total number of valid entries found and the byte length
    /// of valid data (for crash-recovery truncation).
    pub fn rebuild<R: Read + Seek>(&mut self, reader: &mut R) -> io::Result<(u64, u64)> {
        self.entries.clear();
        self.since_last = 0;

        reader.seek(SeekFrom::Start(0))?;
        let file_len = reader.seek(SeekFrom::End(0))?;
        reader.seek(SeekFrom::Start(0))?;

        let mut count: u64 = 0;
        let mut valid_end: u64 = 0;

        loop {
            if valid_end >= file_len {
                break;
            }
            let start = valid_end;
            let mut hdr = [0u8; 4];
            match reader.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let payload_len = u32::from_be_bytes(hdr) as u64;
            let entry_end = start + HEADER_LEN + payload_len;
            if entry_end > file_len || payload_len < ENTRY_META_LEN {
                // Partial or corrupt record — stop here.
                reader.seek(SeekFrom::Start(valid_end))?;
                break;
            }
            if let Err(_) = reader.seek(SeekFrom::Start(entry_end)) {
                break;
            }

            // Record this entry in the sparse index.
            self.record_if_due(count, start);
            count += 1;
            valid_end = entry_end;
        }

        Ok((count, valid_end))
    }

    /// Number of indexed anchor points.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The configured recording interval.
    pub fn interval(&self) -> u32 {
        self.interval
    }
}

// ---------------------------------------------------------------------------
// Convenience: seek-and-scan helper for Segment::read_entry
// ---------------------------------------------------------------------------

/// Using the sparse index, seek `reader` to the nearest anchor at or
/// before `target_seq`, then skip forward to land exactly on `target_seq`.
///
/// After this call the reader is positioned at the start of the target
/// entry's length header and is ready for a `read_one_entry`.
///
/// This is the function `Segment::read_entry` should call instead of its
/// current `for _ in 0..target_offset { skip_one_entry }` loop.
pub fn seek_to_entry<R: Read + Seek>(
    index: &SparseIndex,
    reader: &mut R,
    target_seq: u64,
) -> io::Result<()> {
    let (start_seq, byte_pos) = index.floor(target_seq).unwrap_or((0, 0));
    reader.seek(SeekFrom::Start(byte_pos))?;

    // Linear scan from the anchor to the target (at most `interval - 1` skips).
    for _ in start_seq..target_seq {
        skip_one_entry(reader)?;
    }
    Ok(())
}

/// Skip one entry in the segment file (read its length, seek past payload).
fn skip_one_entry(r: &mut (impl Read + Seek)) -> io::Result<()> {
    let mut hdr = [0u8; 4];
    r.read_exact(&mut hdr)?;
    let payload_len = u32::from_be_bytes(hdr) as i64;
    r.seek(SeekFrom::Current(payload_len))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a fake segment file with `n` entries, each having a fixed-size
    /// payload so byte positions are predictable.
    fn make_segment_bytes(n: u64, data_len: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        let payload_len = (ENTRY_META_LEN as usize) + data_len;
        for i in 0..n {
            buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
            buf.extend_from_slice(&(1u64).to_be_bytes()); // term
            buf.extend_from_slice(&i.to_be_bytes()); // index
            buf.extend_from_slice(&vec![0xABu8; data_len]); // data
        }
        buf
    }

    fn entry_size(data_len: usize) -> u64 {
        HEADER_LEN + ENTRY_META_LEN + data_len as u64
    }

    #[test]
    fn test_new_index_is_empty() {
        let idx = SparseIndex::new(4);
        assert!(idx.is_empty());
        assert_eq!(idx.floor(0), None);
    }

    #[test]
    fn test_record_interval() {
        let mut idx = SparseIndex::new(4);
        for seq in 0..16u64 {
            idx.record_if_due(seq, seq * 100);
        }
        // Should have recorded entries 0, 4, 8, 12.
        assert_eq!(idx.len(), 4);
        assert_eq!(idx.floor(0), Some((0, 0)));
        assert_eq!(idx.floor(3), Some((0, 0)));
        assert_eq!(idx.floor(4), Some((4, 400)));
        assert_eq!(idx.floor(7), Some((4, 400)));
        assert_eq!(idx.floor(12), Some((12, 1200)));
        assert_eq!(idx.floor(15), Some((12, 1200)));
    }

    #[test]
    fn test_interval_one_indexes_every_entry() {
        let mut idx = SparseIndex::new(1);
        for seq in 0..5u64 {
            idx.record_if_due(seq, seq * 50);
        }
        assert_eq!(idx.len(), 5);
        for seq in 0..5u64 {
            assert_eq!(idx.floor(seq), Some((seq, seq * 50)));
        }
    }

    #[test]
    fn test_truncate_from() {
        let mut idx = SparseIndex::new(4);
        for seq in 0..16u64 {
            idx.record_if_due(seq, seq * 100);
        }
        // Truncate from seq 6: keeps anchors at 0 and 4.
        idx.truncate_from(6);
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.floor(5), Some((4, 400)));
        assert_eq!(idx.floor(6), Some((4, 400))); // still returns floor
    }

    #[test]
    fn test_truncate_on_boundary() {
        let mut idx = SparseIndex::new(4);
        for seq in 0..16u64 {
            idx.record_if_due(seq, seq * 100);
        }
        // Truncate exactly at an anchor point.
        idx.truncate_from(8);
        assert_eq!(idx.len(), 2); // anchors at 0 and 4
    }

    #[test]
    fn test_rebuild_from_segment_bytes() {
        let data_len = 10;
        let n = 20u64;
        let bytes = make_segment_bytes(n, data_len);
        let mut cursor = Cursor::new(bytes);

        let mut idx = SparseIndex::new(4);
        let (count, valid_end) = idx.rebuild(&mut cursor).unwrap();
        assert_eq!(count, n);
        assert_eq!(valid_end, n * entry_size(data_len));
        // Anchors at 0, 4, 8, 12, 16.
        assert_eq!(idx.len(), 5);
    }

    #[test]
    fn test_rebuild_with_trailing_garbage() {
        let data_len = 10;
        let n = 5u64;
        let mut bytes = make_segment_bytes(n, data_len);
        // Append partial garbage.
        bytes.extend_from_slice(&[0xFF, 0x00, 0x01]);
        let mut cursor = Cursor::new(bytes);

        let mut idx = SparseIndex::new(2);
        let (count, valid_end) = idx.rebuild(&mut cursor).unwrap();
        assert_eq!(count, 5);
        assert_eq!(valid_end, 5 * entry_size(data_len));
        // Anchors at 0, 2, 4.
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn test_seek_to_entry() {
        let data_len = 8;
        let n = 20u64;
        let bytes = make_segment_bytes(n, data_len);
        let mut cursor = Cursor::new(bytes.clone());

        let mut idx = SparseIndex::new(4);
        idx.rebuild(&mut cursor).unwrap();

        // Seek to entry 7: anchor at 4, scan forward 3.
        let mut reader = Cursor::new(bytes.clone());
        seek_to_entry(&idx, &mut reader, 7).unwrap();
        let pos = reader.position();
        assert_eq!(pos, 7 * entry_size(data_len));

        // Seek to entry 0: should land at byte 0.
        reader = Cursor::new(bytes.clone());
        seek_to_entry(&idx, &mut reader, 0).unwrap();
        assert_eq!(reader.position(), 0);

        // Seek to entry 12: exact anchor hit, no scan needed.
        reader = Cursor::new(bytes);
        seek_to_entry(&idx, &mut reader, 12).unwrap();
        assert_eq!(reader.position(), 12 * entry_size(data_len));
    }

    #[test]
    fn test_rebuild_then_append_more() {
        let data_len = 4;
        let n = 6u64;
        let bytes = make_segment_bytes(n, data_len);
        let mut cursor = Cursor::new(bytes);

        let mut idx = SparseIndex::new(4);
        let (count, _) = idx.rebuild(&mut cursor).unwrap();
        assert_eq!(count, 6);
        // Anchors at 0, 4.
        assert_eq!(idx.len(), 2);

        // Simulate appending entries 6 and 7.
        let e_size = entry_size(data_len);
        idx.record_if_due(6, 6 * e_size);
        idx.record_if_due(7, 7 * e_size);
        // since_last was 2 after rebuild (entries 4,5 since anchor at 4),
        // then 6 → since_last=3, 7 → since_last=0 (wraps), so entry 8
        // would be the next anchor.
        assert_eq!(idx.len(), 2); // no new anchor yet

        idx.record_if_due(8, 8 * e_size);
        assert_eq!(idx.len(), 3); // anchor at 8
    }
}
