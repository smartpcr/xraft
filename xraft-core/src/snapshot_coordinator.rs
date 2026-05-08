// -----------------------------------------------------------------------
// xraft-core :: snapshot_coordinator
//
// Coordinates snapshot creation, persistence, and recovery for the Raft
// consensus layer.  Snapshots compact the replicated log so that slow
// followers (or new voters) can catch up without replaying the full
// history.
// -----------------------------------------------------------------------

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// Metadata that must travel with every snapshot so that consensus state
/// can be fully restored on recovery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotMetadata {
    /// Index of the last log entry included in this snapshot.
    pub last_included_index: u64,
    /// Term of the last log entry included in this snapshot.
    pub last_included_term: u64,
    /// Leader epoch at the time the snapshot was taken.
    pub leader_epoch: u64,
    /// Set of voter node-ids that were part of the configuration when the
    /// snapshot was created.
    pub voters: Vec<u64>,
}

/// A complete point-in-time snapshot of the replicated state machine,
/// including all consensus metadata required for safe recovery.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    /// Opaque, application-defined state-machine payload.
    pub data: Vec<u8>,
}

/// Thin handle returned after a snapshot has been persisted so callers can
/// reference it without holding the full payload in memory.
#[derive(Clone, Debug)]
pub struct SnapshotHandle {
    pub metadata: SnapshotMetadata,
    pub path: PathBuf,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum SnapshotError {
    Io(io::Error),
    Serialize(String),
    Deserialize(String),
    NotFound(PathBuf),
}

impl From<io::Error> for SnapshotError {
    fn from(e: io::Error) -> Self {
        SnapshotError::Io(e)
    }
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Io(e) => write!(f, "snapshot I/O error: {e}"),
            SnapshotError::Serialize(msg) => write!(f, "snapshot serialization error: {msg}"),
            SnapshotError::Deserialize(msg) => write!(f, "snapshot deserialization error: {msg}"),
            SnapshotError::NotFound(p) => write!(f, "snapshot not found: {}", p.display()),
        }
    }
}

impl std::error::Error for SnapshotError {}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

/// Manages snapshot lifecycle on a single node.
///
/// Snapshots are stored as single files under `snapshot_dir`.  The file
/// format is the bincode-serialised `Snapshot` struct (metadata **and**
/// data), which guarantees that recovery always restores the full
/// consensus state.
pub struct SnapshotCoordinator {
    snapshot_dir: PathBuf,
}

impl SnapshotCoordinator {
    /// Create a new coordinator that stores snapshots under `snapshot_dir`.
    /// The directory is created if it does not already exist.
    pub fn new(snapshot_dir: impl Into<PathBuf>) -> Result<Self, SnapshotError> {
        let snapshot_dir = snapshot_dir.into();
        fs::create_dir_all(&snapshot_dir)?;
        Ok(Self { snapshot_dir })
    }

    // -- persistence --------------------------------------------------------

    /// Persist the **entire** [`Snapshot`] (metadata + data) to stable
    /// storage.
    ///
    /// The file is written atomically (write-to-temp then rename) so that a
    /// crash mid-write never leaves a half-written snapshot on disk.
    ///
    /// Returns a lightweight [`SnapshotHandle`] referencing the persisted
    /// file.
    pub fn persist_snapshot(&self, snapshot: &Snapshot) -> Result<SnapshotHandle, SnapshotError> {
        let file_name = format!(
            "snapshot-{}-{}.bin",
            snapshot.metadata.last_included_index, snapshot.metadata.last_included_term,
        );
        let final_path = self.snapshot_dir.join(&file_name);
        let tmp_path = self.snapshot_dir.join(format!("{file_name}.tmp"));

        // Serialize the complete Snapshot — metadata AND data — so that
        // recovery can restore full consensus state.
        let serialized = bincode::serialize(snapshot)
            .map_err(|e| SnapshotError::Serialize(e.to_string()))?;

        // Atomic write: temp file → fsync → rename.
        {
            let mut f = fs::File::create(&tmp_path)?;
            f.write_all(&serialized)?;
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;

        Ok(SnapshotHandle {
            metadata: snapshot.metadata.clone(),
            path: final_path,
        })
    }

    /// Load a snapshot from disk, returning the full [`Snapshot`] with
    /// metadata intact.
    pub fn load_snapshot(&self, path: &Path) -> Result<Snapshot, SnapshotError> {
        if !path.exists() {
            return Err(SnapshotError::NotFound(path.to_path_buf()));
        }
        let bytes = fs::read(path)?;
        let snapshot: Snapshot = bincode::deserialize(&bytes)
            .map_err(|e| SnapshotError::Deserialize(e.to_string()))?;
        Ok(snapshot)
    }

    /// Return the most recent snapshot on disk (by `last_included_index`),
    /// if any.
    pub fn latest_snapshot(&self) -> Result<Option<Snapshot>, SnapshotError> {
        let mut best: Option<(u64, PathBuf)> = None;

        for entry in fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(index) = Self::parse_snapshot_index(&name_str) {
                match &best {
                    Some((cur, _)) if index <= *cur => {}
                    _ => best = Some((index, entry.path())),
                }
            }
        }

        match best {
            Some((_, path)) => self.load_snapshot(&path).map(Some),
            None => Ok(None),
        }
    }

    /// Remove snapshot files whose `last_included_index` is strictly less
    /// than `up_to_index`.
    pub fn gc(&self, up_to_index: u64) -> Result<usize, SnapshotError> {
        let mut removed = 0usize;
        for entry in fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(idx) = Self::parse_snapshot_index(&name_str) {
                if idx < up_to_index {
                    fs::remove_file(entry.path())?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    // -- helpers ------------------------------------------------------------

    /// Extract `last_included_index` from a filename of the form
    /// `snapshot-{index}-{term}.bin`.
    fn parse_snapshot_index(name: &str) -> Option<u64> {
        let stem = name.strip_prefix("snapshot-")?.strip_suffix(".bin")?;
        let idx_str = stem.split('-').next()?;
        idx_str.parse().ok()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_snapshot(index: u64, term: u64) -> Snapshot {
        Snapshot {
            metadata: SnapshotMetadata {
                last_included_index: index,
                last_included_term: term,
                leader_epoch: 3,
                voters: vec![1, 2, 3],
            },
            data: vec![0xCA, 0xFE, 0xBA, 0xBE],
        }
    }

    #[test]
    fn persist_and_load_round_trips_metadata_and_data() {
        let dir = TempDir::new().unwrap();
        let coord = SnapshotCoordinator::new(dir.path()).unwrap();

        let snap = sample_snapshot(42, 5);
        let handle = coord.persist_snapshot(&snap).unwrap();

        // Handle carries correct metadata.
        assert_eq!(handle.metadata, snap.metadata);

        // Full round-trip: metadata survives serialization.
        let recovered = coord.load_snapshot(&handle.path).unwrap();
        assert_eq!(recovered.metadata, snap.metadata);
        assert_eq!(recovered.data, snap.data);
    }

    #[test]
    fn latest_snapshot_returns_highest_index() {
        let dir = TempDir::new().unwrap();
        let coord = SnapshotCoordinator::new(dir.path()).unwrap();

        coord.persist_snapshot(&sample_snapshot(10, 1)).unwrap();
        coord.persist_snapshot(&sample_snapshot(50, 3)).unwrap();
        coord.persist_snapshot(&sample_snapshot(30, 2)).unwrap();

        let latest = coord.latest_snapshot().unwrap().unwrap();
        assert_eq!(latest.metadata.last_included_index, 50);
        assert_eq!(latest.metadata.last_included_term, 3);
    }

    #[test]
    fn gc_removes_old_snapshots() {
        let dir = TempDir::new().unwrap();
        let coord = SnapshotCoordinator::new(dir.path()).unwrap();

        coord.persist_snapshot(&sample_snapshot(10, 1)).unwrap();
        coord.persist_snapshot(&sample_snapshot(20, 2)).unwrap();
        coord.persist_snapshot(&sample_snapshot(30, 3)).unwrap();

        let removed = coord.gc(25).unwrap();
        assert_eq!(removed, 2);

        let latest = coord.latest_snapshot().unwrap().unwrap();
        assert_eq!(latest.metadata.last_included_index, 30);
    }
}
