// -----------------------------------------------------------------------
// xraft-storage :: snapshot_store
//
// Persistent snapshot storage for the Raft consensus protocol.
// Snapshots are stored as files on disk, one directory per snapshot,
// named by offset and term: `snapshot-{offset}-{term}/`.
// -----------------------------------------------------------------------

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Metadata describing a single Raft snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// The last log offset (index) included in this snapshot.
    pub offset: u64,
    /// The term of the last log entry included in this snapshot.
    pub term: u64,
    /// Number of cluster members at snapshot time.
    pub cluster_size: u32,
}

/// A handle to a snapshot: metadata + the raw payload bytes.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

/// Errors surfaced by the snapshot store.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("corrupt snapshot directory name: {0}")]
    BadDirName(String),

    #[error("no snapshots found in store")]
    NoneFound,

    #[error("snapshot not found for offset={0} term={1}")]
    NotFound(u64, u64),
}

pub type Result<T> = std::result::Result<T, SnapshotError>;

/// File-system backed snapshot store.
///
/// Layout on disk:
/// ```text
/// <root>/
///   snapshot-00000012-00000003/
///     meta.json
///     data.bin
///   snapshot-00000018-00000004/
///     meta.json
///     data.bin
/// ```
pub struct SnapshotStore {
    root: PathBuf,
}

impl SnapshotStore {
    /// Open (or create) a snapshot store rooted at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Return the directory name for a given offset/term pair.
    fn dir_name(offset: u64, term: u64) -> String {
        format!("snapshot-{:08x}-{:08x}", offset, term)
    }

    /// Parse offset and term from a snapshot directory name.
    fn parse_dir_name(name: &str) -> std::result::Result<(u64, u64), SnapshotError> {
        let rest = name
            .strip_prefix("snapshot-")
            .ok_or_else(|| SnapshotError::BadDirName(name.to_string()))?;
        let parts: Vec<&str> = rest.splitn(2, '-').collect();
        if parts.len() != 2 {
            return Err(SnapshotError::BadDirName(name.to_string()));
        }
        let offset = u64::from_str_radix(parts[0], 16)
            .map_err(|_| SnapshotError::BadDirName(name.to_string()))?;
        let term = u64::from_str_radix(parts[1], 16)
            .map_err(|_| SnapshotError::BadDirName(name.to_string()))?;
        Ok((offset, term))
    }

    /// Persist a new snapshot to disk.
    pub fn save(&self, snapshot: &Snapshot) -> Result<PathBuf> {
        let dir = self
            .root
            .join(Self::dir_name(snapshot.meta.offset, snapshot.meta.term));
        fs::create_dir_all(&dir)?;

        // Write metadata.
        let meta_path = dir.join("meta.json");
        let meta_json = format!(
            r#"{{"offset":{},"term":{},"cluster_size":{}}}"#,
            snapshot.meta.offset, snapshot.meta.term, snapshot.meta.cluster_size,
        );
        fs::write(&meta_path, meta_json.as_bytes())?;

        // Write payload.
        let data_path = dir.join("data.bin");
        let mut f = fs::File::create(&data_path)?;
        f.write_all(&snapshot.data)?;
        f.sync_all()?;

        Ok(dir)
    }

    /// List all snapshot metadata present in the store, sorted by
    /// `(offset, term)` ascending.
    pub fn list(&self) -> Result<Vec<SnapshotMeta>> {
        let mut metas = Vec::new();

        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.starts_with("snapshot-") {
                continue;
            }
            let (offset, term) = Self::parse_dir_name(&name_str)?;
            let meta = self.read_meta(offset, term)?;
            metas.push(meta);
        }

        metas.sort_by_key(|m| (m.offset, m.term));
        Ok(metas)
    }

    /// Load a specific snapshot by offset and term.
    pub fn load(&self, offset: u64, term: u64) -> Result<Snapshot> {
        let dir = self.root.join(Self::dir_name(offset, term));
        if !dir.exists() {
            return Err(SnapshotError::NotFound(offset, term));
        }
        let meta = self.read_meta(offset, term)?;
        let data = fs::read(dir.join("data.bin"))?;
        Ok(Snapshot { meta, data })
    }

    /// Load the latest snapshot — the one with the highest offset and,
    /// when offsets are equal, the highest term.
    ///
    /// This deterministic tie-breaking on `(offset, term)` is required
    /// because two snapshots may share the same offset after a leader
    /// change that occurs before log compaction.
    pub fn load_latest(&self) -> Result<Snapshot> {
        let mut best: Option<(u64, u64)> = None;

        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.starts_with("snapshot-") {
                continue;
            }
            let (offset, term) = Self::parse_dir_name(&name_str)?;

            let dominated = match best {
                Some((best_offset, best_term)) => {
                    // Primary: highest offset wins.
                    // Secondary: highest term breaks ties.
                    (offset, term) > (best_offset, best_term)
                }
                None => true,
            };

            if dominated {
                best = Some((offset, term));
            }
        }

        match best {
            Some((offset, term)) => self.load(offset, term),
            None => Err(SnapshotError::NoneFound),
        }
    }

    /// Remove all snapshots whose offset is strictly less than
    /// `min_offset`. Returns the number of directories removed.
    pub fn purge_before(&self, min_offset: u64) -> Result<usize> {
        let mut removed = 0usize;

        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.starts_with("snapshot-") {
                continue;
            }
            let (offset, _term) = Self::parse_dir_name(&name_str)?;
            if offset < min_offset {
                fs::remove_dir_all(entry.path())?;
                removed += 1;
            }
        }

        Ok(removed)
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn read_meta(&self, offset: u64, term: u64) -> Result<SnapshotMeta> {
        let dir = self.root.join(Self::dir_name(offset, term));
        let meta_path = dir.join("meta.json");
        let mut buf = String::new();
        fs::File::open(&meta_path)?.read_to_string(&mut buf)?;

        // Minimal JSON parsing — avoids pulling in serde for this leaf crate.
        let cluster_size = Self::extract_json_u64(&buf, "cluster_size")
            .unwrap_or(0) as u32;

        Ok(SnapshotMeta {
            offset,
            term,
            cluster_size,
        })
    }

    fn extract_json_u64(json: &str, key: &str) -> Option<u64> {
        let needle = format!("\"{}\":", key);
        let start = json.find(&needle)? + needle.len();
        let rest = json[start..].trim_start();
        let end = rest.find(|c: char| !c.is_ascii_digit())?;
        rest[..end].parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_snapshot(offset: u64, term: u64, payload: &[u8]) -> Snapshot {
        Snapshot {
            meta: SnapshotMeta {
                offset,
                term,
                cluster_size: 3,
            },
            data: payload.to_vec(),
        }
    }

    #[test]
    fn round_trip_save_load() {
        let tmp = TempDir::new().unwrap();
        let store = SnapshotStore::open(tmp.path()).unwrap();

        let snap = make_snapshot(10, 2, b"hello raft");
        store.save(&snap).unwrap();

        let loaded = store.load(10, 2).unwrap();
        assert_eq!(loaded.meta, snap.meta);
        assert_eq!(loaded.data, snap.data);
    }

    #[test]
    fn load_latest_picks_highest_offset() {
        let tmp = TempDir::new().unwrap();
        let store = SnapshotStore::open(tmp.path()).unwrap();

        store.save(&make_snapshot(5, 1, b"old")).unwrap();
        store.save(&make_snapshot(20, 3, b"new")).unwrap();
        store.save(&make_snapshot(12, 2, b"mid")).unwrap();

        let latest = store.load_latest().unwrap();
        assert_eq!(latest.meta.offset, 20);
        assert_eq!(latest.meta.term, 3);
    }

    #[test]
    fn load_latest_breaks_tie_on_term() {
        let tmp = TempDir::new().unwrap();
        let store = SnapshotStore::open(tmp.path()).unwrap();

        // Two snapshots at the same offset but different terms —
        // the one with the higher term must always win.
        store.save(&make_snapshot(15, 2, b"term2")).unwrap();
        store.save(&make_snapshot(15, 5, b"term5")).unwrap();
        store.save(&make_snapshot(15, 3, b"term3")).unwrap();

        let latest = store.load_latest().unwrap();
        assert_eq!(latest.meta.offset, 15);
        assert_eq!(latest.meta.term, 5);
        assert_eq!(latest.data, b"term5");
    }

    #[test]
    fn list_returns_sorted() {
        let tmp = TempDir::new().unwrap();
        let store = SnapshotStore::open(tmp.path()).unwrap();

        store.save(&make_snapshot(20, 4, b"c")).unwrap();
        store.save(&make_snapshot(10, 2, b"a")).unwrap();
        store.save(&make_snapshot(10, 3, b"b")).unwrap();

        let metas = store.list().unwrap();
        let keys: Vec<(u64, u64)> = metas.iter().map(|m| (m.offset, m.term)).collect();
        assert_eq!(keys, vec![(10, 2), (10, 3), (20, 4)]);
    }

    #[test]
    fn purge_removes_old_snapshots() {
        let tmp = TempDir::new().unwrap();
        let store = SnapshotStore::open(tmp.path()).unwrap();

        store.save(&make_snapshot(5, 1, b"a")).unwrap();
        store.save(&make_snapshot(10, 2, b"b")).unwrap();
        store.save(&make_snapshot(20, 3, b"c")).unwrap();

        let removed = store.purge_before(10).unwrap();
        assert_eq!(removed, 1);

        let remaining = store.list().unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().all(|m| m.offset >= 10));
    }

    #[test]
    fn load_latest_empty_store() {
        let tmp = TempDir::new().unwrap();
        let store = SnapshotStore::open(tmp.path()).unwrap();

        assert!(matches!(
            store.load_latest(),
            Err(SnapshotError::NoneFound)
        ));
    }
}
