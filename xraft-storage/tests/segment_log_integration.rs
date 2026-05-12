//! Integration tests for the segment log write path (Stage 2.1).
//!
//! These tests exercise the public API of `SegmentLog` through the `LogStore`
//! trait, verifying the three required scenarios:
//!   1. Append and read back 100 entries
//!   2. CRC integrity — corruption produces `StorageError`
//!   3. Segment rollover at a 1 KB size limit

use std::fs;
use std::io::{Read, Seek, Write};

use tempfile::TempDir;

use xraft_core::error::XraftError;
use xraft_core::log_entry::{EntryType, LogEntry};
use xraft_core::traits::LogStore;
use xraft_core::types::{ClusterId, Term};
use xraft_storage::{SegmentLog, SegmentLogConfig};

fn make_entry(offset: u64, term: u64, payload: &[u8]) -> LogEntry {
    LogEntry {
        offset,
        term: Term(term),
        entry_type: EntryType::Command,
        payload: payload.to_vec(),
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: Append and read back
// ---------------------------------------------------------------------------

/// Given an empty segment log, When 100 entries are appended,
/// Then `read(0, 100)` returns all 100 entries with matching offsets, terms,
/// and payloads.
#[tokio::test]
async fn append_and_read_back_100_entries() {
    let dir = TempDir::new().unwrap();
    let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

    let entries: Vec<LogEntry> = (0..100)
        .map(|i| make_entry(i, 1 + i / 25, &format!("data-{i}").into_bytes()))
        .collect();

    log.append(&entries).await.unwrap();

    assert_eq!(log.log_start_offset(), 0);
    assert_eq!(log.log_end_offset(), 100);

    let read_back = log.read(0, 100).await.unwrap();
    assert_eq!(read_back.len(), 100);

    for (i, entry) in read_back.iter().enumerate() {
        let i = i as u64;
        assert_eq!(entry.offset, i, "offset mismatch at index {i}");
        assert_eq!(entry.term, Term(1 + i / 25), "term mismatch at index {i}");
        assert_eq!(
            entry.payload,
            format!("data-{i}").into_bytes(),
            "payload mismatch at index {i}"
        );
        assert_eq!(entry.entry_type, EntryType::Command);
    }
}

/// Verify `entry_at` returns the correct single entry.
#[tokio::test]
async fn entry_at_returns_matching_entry() {
    let dir = TempDir::new().unwrap();
    let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

    let entries: Vec<LogEntry> = (0..50)
        .map(|i| make_entry(i, i + 1, &[i as u8; 8]))
        .collect();
    log.append(&entries).await.unwrap();

    for i in [0u64, 1, 24, 25, 49] {
        let entry = log.entry_at(i).await.unwrap().expect("entry should exist");
        assert_eq!(entry.offset, i);
        assert_eq!(entry.term, Term(i + 1));
        assert_eq!(entry.payload, vec![i as u8; 8]);
    }

    // Out-of-bounds returns None
    assert!(log.entry_at(50).await.unwrap().is_none());
    assert!(log.entry_at(999).await.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Scenario 2: CRC integrity
// ---------------------------------------------------------------------------

/// Given a segment file, When a byte is corrupted mid-segment,
/// Then reading past the corruption point returns a `StorageError`.
///
/// This test exercises the public `LogStore::read` API on a **live**
/// `SegmentLog` handle (no reopen/recovery). The batch CRC check inside
/// `Segment::read` must detect the corrupted byte and return
/// `StorageError(InvalidData)`.
#[tokio::test]
async fn crc_integrity_returns_storage_error() {
    let dir = TempDir::new().unwrap();
    let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

    // Write 10 entries as 10 individual batches so each has its own CRC.
    for i in 0..10u64 {
        log.append(&[make_entry(i, 1, &[i as u8; 32])])
            .await
            .unwrap();
    }
    assert_eq!(log.log_end_offset(), 10);

    // Corrupt batch 5's payload bytes on disk (without reopening the SegmentLog).
    let log_path = dir.path().join("00000000000000000000.log");
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)
            .unwrap();
        let mut raw = Vec::new();
        f.read_to_end(&mut raw).unwrap();
        // Each batch is the same size; corrupt the entry data inside batch 5.
        let batch_size = raw.len() / 10;
        let corrupt_pos = batch_size * 5 + 14; // inside entry data
        raw[corrupt_pos] ^= 0xFF;
        f.seek(std::io::SeekFrom::Start(0)).unwrap();
        f.write_all(&raw).unwrap();
        f.sync_all().unwrap();
    }

    // Reading through the live SegmentLog must return StorageError(InvalidData)
    let result = log.read(0, 10).await;
    match result {
        Err(XraftError::StorageError(ref e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
            assert!(
                e.to_string().contains("CRC"),
                "expected CRC error, got: {e}"
            );
        }
        Ok(_) => panic!("expected StorageError from CRC mismatch, got Ok"),
        Err(other) => panic!("expected StorageError, got: {other}"),
    }
}

/// A second CRC scenario: reading only entries *before* the corruption
/// succeeds; reading *past* the corruption offset returns an error.
#[tokio::test]
async fn crc_integrity_partial_read_before_corruption_succeeds() {
    let dir = TempDir::new().unwrap();
    let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

    for i in 0..10u64 {
        log.append(&[make_entry(i, 1, &[i as u8; 32])])
            .await
            .unwrap();
    }

    // Corrupt batch 7 on disk
    let log_path = dir.path().join("00000000000000000000.log");
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)
            .unwrap();
        let mut raw = Vec::new();
        f.read_to_end(&mut raw).unwrap();
        let batch_size = raw.len() / 10;
        let corrupt_pos = batch_size * 7 + 14;
        raw[corrupt_pos] ^= 0xFF;
        f.seek(std::io::SeekFrom::Start(0)).unwrap();
        f.write_all(&raw).unwrap();
        f.sync_all().unwrap();
    }

    // Reading entries 0..5 (before corruption) should succeed
    let before = log.read(0, 5).await.unwrap();
    assert_eq!(before.len(), 5);
    for (i, entry) in before.iter().enumerate() {
        assert_eq!(entry.offset, i as u64);
    }

    // Reading entries 0..10 (spans corruption) must fail
    let result = log.read(0, 10).await;
    assert!(
        matches!(result, Err(XraftError::StorageError(_))),
        "expected StorageError spanning corruption, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Scenario 3: Segment rollover
// ---------------------------------------------------------------------------

/// Given a segment size limit of 1 KB, When entries exceeding 1 KB total are
/// appended, Then a new segment file is created.
#[tokio::test]
async fn segment_rollover_at_1kb() {
    let dir = TempDir::new().unwrap();
    let config = SegmentLogConfig {
        max_segment_size: 1024,
        index_interval: 4,
    };
    let log = SegmentLog::open(dir.path(), config).unwrap();

    // Each entry with a 32-byte payload serializes to ~50+ bytes per record.
    // Writing 100 entries in batches of 5 forces multiple rollovers.
    for batch in 0..20 {
        let start = batch * 5;
        let entries: Vec<LogEntry> = (start..start + 5)
            .map(|i| make_entry(i, 1, &[0xCDu8; 32]))
            .collect();
        log.append(&entries).await.unwrap();
    }

    assert_eq!(log.log_end_offset(), 100);

    // Count .log files — must be more than 1
    let log_files: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                == Some("log")
        })
        .collect();
    assert!(
        log_files.len() > 1,
        "expected multiple segment files from rollover, got {}",
        log_files.len()
    );

    // Matching .index files should exist for each .log file
    let idx_files: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                == Some("index")
        })
        .collect();
    assert_eq!(
        log_files.len(),
        idx_files.len(),
        "each segment should have a matching .index file"
    );

    // All files should use 20-digit zero-padded naming
    for f in &log_files {
        let stem = f.path().file_stem().unwrap().to_str().unwrap().to_owned();
        assert_eq!(stem.len(), 20, "segment name should be 20 digits: {stem}");
        assert!(
            stem.chars().all(|c| c.is_ascii_digit()),
            "segment name should be all digits: {stem}"
        );
    }

    // Read all 100 entries back across segments — full round-trip
    let read_back = log.read(0, 100).await.unwrap();
    assert_eq!(read_back.len(), 100);
    for (i, entry) in read_back.iter().enumerate() {
        assert_eq!(entry.offset, i as u64);
        assert_eq!(entry.term, Term(1));
        assert_eq!(entry.payload, vec![0xCDu8; 32]);
    }
}

/// A single `append` call with entries totaling far more than 1 KB must
/// produce multiple segments by splitting the batch internally.
#[tokio::test]
async fn segment_rollover_single_append_call() {
    let dir = TempDir::new().unwrap();
    let config = SegmentLogConfig {
        max_segment_size: 1024, // 1 KB limit
        index_interval: 4,
    };
    let log = SegmentLog::open(dir.path(), config).unwrap();

    // 100 entries with 32-byte payloads ≈ 6+ KB total — well over 1 KB.
    // A single `append` call must split across multiple segments.
    let entries: Vec<LogEntry> = (0..100)
        .map(|i| make_entry(i, 1, &[0xAAu8; 32]))
        .collect();
    log.append(&entries).await.unwrap();

    assert_eq!(log.log_end_offset(), 100);

    // Must have created more than 1 segment
    let log_files: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                == Some("log")
        })
        .collect();
    assert!(
        log_files.len() > 1,
        "single append of ~6KB with 1KB limit must produce multiple segments, got {}",
        log_files.len()
    );

    // No segment file should wildly exceed the limit
    for f in &log_files {
        let size = f.metadata().unwrap().len();
        assert!(
            size <= 1024 + 200,
            "segment {} is {} bytes, exceeds 1 KB + tolerance",
            f.path().display(),
            size
        );
    }

    // Full round-trip: read all entries back
    let read_back = log.read(0, 100).await.unwrap();
    assert_eq!(read_back.len(), 100);
    for (i, entry) in read_back.iter().enumerate() {
        assert_eq!(entry.offset, i as u64);
    }
}

// ---------------------------------------------------------------------------
// Directory layout
// ---------------------------------------------------------------------------

/// Verify the canonical directory layout via `SegmentLog::open_for_cluster`:
/// `data/<cluster_id>/log/` with properly named `.log` and `.index` files.
#[tokio::test]
async fn directory_layout_with_cluster_id() {
    let dir = TempDir::new().unwrap();
    let cluster_id = ClusterId::random();

    let log = SegmentLog::open_for_cluster(
        dir.path(),
        &cluster_id,
        SegmentLogConfig::default(),
    )
    .unwrap();

    let entries = vec![make_entry(0, 1, b"init")];
    log.append(&entries).await.unwrap();

    let log_dir = dir
        .path()
        .join("data")
        .join(cluster_id.as_str())
        .join("log");
    assert!(log_dir.exists(), "cluster log directory should be created");
    assert!(
        log_dir.join("00000000000000000000.log").exists(),
        "first .log file should exist"
    );
    assert!(
        log_dir.join("00000000000000000000.index").exists(),
        "first .index file should exist"
    );
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

/// Appending batches across segment boundaries still maintains offset continuity.
#[tokio::test]
async fn cross_segment_read_continuity() {
    let dir = TempDir::new().unwrap();
    let config = SegmentLogConfig {
        max_segment_size: 512,
        index_interval: 2,
    };
    let log = SegmentLog::open(dir.path(), config).unwrap();

    for batch in 0..30 {
        let start = batch * 3;
        let entries: Vec<LogEntry> = (start..start + 3)
            .map(|i| make_entry(i, 1 + i / 10, &[0xABu8; 16]))
            .collect();
        log.append(&entries).await.unwrap();
    }

    assert_eq!(log.log_end_offset(), 90);

    // Read a range that spans multiple segments
    let mid = log.read(30, 60).await.unwrap();
    assert_eq!(mid.len(), 30);
    for (j, entry) in mid.iter().enumerate() {
        let expected_offset = 30 + j as u64;
        assert_eq!(entry.offset, expected_offset);
    }
}

/// Re-opening a multi-segment log preserves all data.
#[tokio::test]
async fn reopen_preserves_multi_segment_data() {
    let dir = TempDir::new().unwrap();
    let config = SegmentLogConfig {
        max_segment_size: 1024,
        index_interval: 4,
    };

    // Write entries, then drop the log
    {
        let log = SegmentLog::open(dir.path(), config).unwrap();
        for batch in 0..10 {
            let start = batch * 10;
            let entries: Vec<LogEntry> = (start..start + 10)
                .map(|i| make_entry(i, 2, &[i as u8; 24]))
                .collect();
            log.append(&entries).await.unwrap();
        }
        assert_eq!(log.log_end_offset(), 100);
    }

    // Re-open and verify
    let config2 = SegmentLogConfig {
        max_segment_size: 1024,
        index_interval: 4,
    };
    let log = SegmentLog::open(dir.path(), config2).unwrap();
    assert_eq!(log.log_end_offset(), 100);

    let all = log.read(0, 100).await.unwrap();
    assert_eq!(all.len(), 100);
    for (i, entry) in all.iter().enumerate() {
        assert_eq!(entry.offset, i as u64);
        assert_eq!(entry.term, Term(2));
        assert_eq!(entry.payload, vec![i as u8; 24]);
    }
}
