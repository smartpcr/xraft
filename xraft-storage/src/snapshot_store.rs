//! File-based snapshot store implementing `SnapshotIO`.
//!
//! Snapshot files are stored at:
//!   `data/<cluster_id>/log/snapshot/<offset>-<term>.snap`
//!
//! On-disk format (v1):
//!   [4 bytes: magic 0x58534E50 "XSNP"]
//!   [1 byte:  version = 1]
//!   [4 bytes: CRC32 of payload]
//!   [N bytes: bincode-serialized Snapshot]

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use xraft_core::{
    ClusterId, Snapshot, SnapshotIO, SnapshotId, SnapshotWriter, SnapshotWriterInner, Term,
};

const MAGIC: [u8; 4] = [0x58, 0x53, 0x4E, 0x50]; // "XSNP"
const FORMAT_VERSION: u8 = 1;
const HEADER_SIZE: usize = 4 + 1 + 4; // magic + version + crc32

/// File-based implementation of `SnapshotIO`.
///
/// All write operations use the atomic pattern: write to temp file → fsync →
/// rename → fsync parent directory. Read operations are lock-free because
/// snapshot files are immutable once written.
pub struct FileSnapshotStore {
    snapshot_dir: PathBuf,
}

impl FileSnapshotStore {
    /// Create a new `FileSnapshotStore` for the given cluster.
    ///
    /// Creates the snapshot directory if it does not exist.
    pub async fn new(base_dir: &Path, cluster_id: &ClusterId) -> std::io::Result<Self> {
        let snapshot_dir = base_dir
            .join("data")
            .join(cluster_id.to_string())
            .join("log")
            .join("snapshot");
        fs::create_dir_all(&snapshot_dir).await?;
        Ok(Self { snapshot_dir })
    }

    /// Build the filename for a snapshot: `<offset>-<term>.snap`
    fn snap_filename(offset: u64, term: &Term) -> String {
        format!("{}-{}.snap", offset, term.0)
    }

    /// Parse a snapshot filename into (offset, term). Returns `None` for
    /// non-matching filenames (including `.tmp` and other debris).
    fn parse_snap_filename(name: &str) -> Option<(u64, Term)> {
        let stem = name.strip_suffix(".snap")?;
        let (offset_s, term_s) = stem.split_once('-')?;
        let offset = offset_s.parse::<u64>().ok()?;
        let term = term_s.parse::<u64>().ok()?;
        Some((offset, Term(term)))
    }

    /// Encode a `Snapshot` into the on-disk format (header + bincode payload).
    fn encode(snapshot: &Snapshot) -> Result<Vec<u8>, bincode::Error> {
        let payload = bincode::serialize(snapshot)?;
        let crc = crc32fast::hash(&payload);
        let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
        buf.extend_from_slice(&MAGIC);
        buf.push(FORMAT_VERSION);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// Decode on-disk bytes back into a `Snapshot`, verifying magic + CRC.
    fn decode(data: &[u8]) -> anyhow::Result<Snapshot> {
        if data.len() < HEADER_SIZE {
            anyhow::bail!("snapshot file too small ({} bytes)", data.len());
        }
        if data[..4] != MAGIC {
            anyhow::bail!("invalid snapshot magic");
        }
        if data[4] != FORMAT_VERSION {
            anyhow::bail!("unsupported snapshot format version {}", data[4]);
        }
        let stored_crc = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);
        let payload = &data[HEADER_SIZE..];
        let actual_crc = crc32fast::hash(payload);
        if stored_crc != actual_crc {
            anyhow::bail!(
                "CRC mismatch: stored={:#010x}, computed={:#010x}",
                stored_crc,
                actual_crc
            );
        }
        let snapshot: Snapshot = bincode::deserialize(payload)?;
        Ok(snapshot)
    }

    /// Write data to a file atomically: temp file → fsync → rename → fsync dir.
    async fn atomic_write(&self, final_path: &Path, data: &[u8]) -> std::io::Result<()> {
        let tmp_path = final_path.with_extension("snap.tmp");

        // Write to temp file
        let mut file = fs::File::create(&tmp_path).await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        drop(file);

        // Atomic rename
        fs::rename(&tmp_path, final_path).await?;

        // Fsync parent directory for rename durability
        Self::fsync_dir(&self.snapshot_dir).await?;

        Ok(())
    }

    /// Fsync a directory to ensure metadata operations (rename) are durable.
    #[cfg(unix)]
    async fn fsync_dir(dir: &Path) -> std::io::Result<()> {
        let dir = dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let f = std::fs::File::open(&dir)?;
            f.sync_all()
        })
        .await?
    }

    /// On Windows, directory fsync is not directly supported; FlushFileBuffers
    /// does not apply to directories. We rely on NTFS journaling + the file
    /// fsync above for durability.
    #[cfg(not(unix))]
    async fn fsync_dir(_dir: &Path) -> std::io::Result<()> {
        Ok(())
    }

    /// Build the full path for a snapshot file.
    fn snap_path(&self, offset: u64, term: &Term) -> PathBuf {
        self.snapshot_dir
            .join(Self::snap_filename(offset, term))
    }
}

#[async_trait]
impl SnapshotIO for FileSnapshotStore {
    /// Write a complete snapshot atomically.
    async fn save(&self, snapshot: &Snapshot) -> anyhow::Result<()> {
        let offset = snapshot.metadata.last_included_offset;
        let term = &snapshot.metadata.last_included_term;
        let final_path = self.snap_path(offset, term);
        let data = Self::encode(snapshot)?;
        self.atomic_write(&final_path, &data).await?;
        Ok(())
    }

    /// Scan the snapshot directory, parse filenames, and return the snapshot
    /// with the highest `last_included_offset`. Ignores `.tmp` files and
    /// malformed filenames.
    async fn load_latest(&self) -> anyhow::Result<Option<Snapshot>> {
        let mut best: Option<(u64, Term, PathBuf)> = None;

        let mut entries = fs::read_dir(&self.snapshot_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if let Some((offset, term)) = Self::parse_snap_filename(&name) {
                match &best {
                    Some((best_offset, _, _)) if offset <= *best_offset => {}
                    _ => {
                        best = Some((offset, term, entry.path()));
                    }
                }
            }
        }

        match best {
            Some((_, _, path)) => {
                let data = fs::read(&path).await?;
                let snapshot = Self::decode(&data)?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    /// Read a chunk of the snapshot at the given byte position.
    /// Returns `(data, is_last_chunk)`.
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> anyhow::Result<(Bytes, bool)> {
        let path = self.snap_path(id.end_offset, &id.epoch);
        let mut file = fs::File::open(&path).await?;
        let file_len = file.metadata().await?.len();

        if position >= file_len {
            return Ok((Bytes::new(), true));
        }

        file.seek(std::io::SeekFrom::Start(position)).await?;

        let remaining = file_len - position;
        let to_read = std::cmp::min(remaining, max_bytes as u64) as usize;
        let mut buf = vec![0u8; to_read];
        file.read_exact(&mut buf).await?;

        let is_last = position + to_read as u64 >= file_len;
        Ok((Bytes::from(buf), is_last))
    }

    /// Begin receiving a snapshot from the leader. Returns a `SnapshotWriter`
    /// that writes chunks to a temp file and atomically finalizes.
    async fn begin_receive(&self, id: &SnapshotId) -> anyhow::Result<SnapshotWriter> {
        let final_path = self.snap_path(id.end_offset, &id.epoch);
        let tmp_path = final_path.with_extension("snap.tmp");

        // Clean up any stale temp file from a prior interrupted receive
        let _ = fs::remove_file(&tmp_path).await;

        let file = fs::File::create(&tmp_path).await?;
        let snapshot_dir = self.snapshot_dir.clone();

        let writer = FileSnapshotWriter {
            file,
            tmp_path,
            final_path,
            snapshot_dir,
        };

        Ok(SnapshotWriter::new(Box::new(writer)))
    }
}

/// File-backed writer for assembling a snapshot from chunks received from the
/// leader. Writes to a temp file and atomically renames on `finalize()`.
struct FileSnapshotWriter {
    file: fs::File,
    tmp_path: PathBuf,
    final_path: PathBuf,
    snapshot_dir: PathBuf,
}

#[async_trait]
impl SnapshotWriterInner for FileSnapshotWriter {
    async fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.file.write_all(data).await
    }

    async fn finalize(mut self: Box<Self>) -> std::io::Result<()> {
        self.file.sync_all().await?;
        drop(self.file);

        fs::rename(&self.tmp_path, &self.final_path).await?;

        // Fsync parent directory
        FileSnapshotStore::fsync_dir(&self.snapshot_dir).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::{AppSnapshot, SnapshotMetadata, Term, VoterInfo, NodeId};

    fn make_snapshot(offset: u64, term: u64, payload: Vec<u8>) -> Snapshot {
        Snapshot {
            metadata: SnapshotMetadata {
                last_included_offset: offset,
                last_included_term: Term(term),
                voters: vec![
                    VoterInfo {
                        node_id: NodeId(1),
                        endpoint: "127.0.0.1:9000".parse().unwrap(),
                    },
                    VoterInfo {
                        node_id: NodeId(2),
                        endpoint: "127.0.0.1:9001".parse().unwrap(),
                    },
                ],
                leader_epoch: Term(term),
            },
            app_snapshot: AppSnapshot { data: payload },
        }
    }

    #[tokio::test]
    async fn save_and_load_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();

        let payload = b"application state data for round-trip test".to_vec();
        let original = make_snapshot(42, 3, payload);
        store.save(&original).await.unwrap();

        let loaded = store.load_latest().await.unwrap().expect("should find snapshot");
        assert_eq!(loaded, original);
    }

    #[tokio::test]
    async fn chunked_read_10kb() {
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();

        // Create a snapshot; we'll measure the serialized file size and read
        // it back in 1 KB chunks.
        let payload = vec![0xAB; 8192]; // use a large payload
        let snapshot = make_snapshot(100, 5, payload);
        store.save(&snapshot).await.unwrap();

        let id = SnapshotId {
            end_offset: 100,
            epoch: Term(5),
        };

        // Determine the actual file size
        let snap_path = store.snap_path(100, &Term(5));
        let file_len = fs::metadata(&snap_path).await.unwrap().len();

        let chunk_size: u32 = 1024;
        let expected_chunks = ((file_len as f64) / (chunk_size as f64)).ceil() as usize;

        let mut assembled = Vec::new();
        let mut position: u64 = 0;
        let mut chunk_count = 0;

        loop {
            let (data, is_last) = store.read_chunk(&id, position, chunk_size).await.unwrap();
            if !data.is_empty() {
                chunk_count += 1;
                position += data.len() as u64;
                assembled.extend_from_slice(&data);
            }
            if is_last {
                break;
            }
        }

        assert_eq!(chunk_count, expected_chunks);
        assert_eq!(assembled.len(), file_len as usize);

        // Verify the concatenated chunks equal the file on disk
        let file_bytes = fs::read(&snap_path).await.unwrap();
        assert_eq!(assembled, file_bytes);
    }

    #[tokio::test]
    async fn chunked_read_exact_10kb_10_chunks() {
        // Test scenario from spec: 10 KB snapshot → 1 KB chunks → 10 chunks
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();

        // We need the *file* to be exactly 10240 bytes. We serialize a snapshot
        // and then pad the payload so the total file size is exactly 10 KB.
        let probe_snapshot = make_snapshot(200, 7, vec![]);
        let probe_encoded = FileSnapshotStore::encode(&probe_snapshot).unwrap();
        let overhead = probe_encoded.len();
        let target_size: usize = 10240;
        assert!(
            overhead < target_size,
            "metadata overhead ({overhead}) exceeds 10 KB — adjust test"
        );
        let payload_size = target_size - overhead;
        let snapshot = make_snapshot(200, 7, vec![0xCD; payload_size]);

        // Verify our math produces exactly 10 KB on disk
        let encoded = FileSnapshotStore::encode(&snapshot).unwrap();
        assert_eq!(encoded.len(), target_size);

        store.save(&snapshot).await.unwrap();

        let id = SnapshotId {
            end_offset: 200,
            epoch: Term(7),
        };

        let chunk_size: u32 = 1024;
        let mut assembled = Vec::new();
        let mut position: u64 = 0;
        let mut chunk_count = 0;

        loop {
            let (data, is_last) = store.read_chunk(&id, position, chunk_size).await.unwrap();
            if !data.is_empty() {
                chunk_count += 1;
                position += data.len() as u64;
                assembled.extend_from_slice(&data);
            }
            if is_last {
                break;
            }
        }

        assert_eq!(chunk_count, 10, "expected exactly 10 chunks for 10 KB file");
        assert_eq!(assembled.len(), target_size);

        // Verify round-trip
        let decoded = FileSnapshotStore::decode(&assembled).unwrap();
        assert_eq!(decoded, snapshot);
    }

    #[tokio::test]
    async fn latest_snapshot_selection() {
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();

        let snap_100 = make_snapshot(100, 2, b"state@100".to_vec());
        let snap_500 = make_snapshot(500, 4, b"state@500".to_vec());
        let snap_300 = make_snapshot(300, 3, b"state@300".to_vec());

        store.save(&snap_100).await.unwrap();
        store.save(&snap_500).await.unwrap();
        store.save(&snap_300).await.unwrap();

        let latest = store.load_latest().await.unwrap().expect("should find latest");
        assert_eq!(latest.metadata.last_included_offset, 500);
        assert_eq!(latest, snap_500);
    }

    #[tokio::test]
    async fn load_latest_returns_none_when_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();
        let result = store.load_latest().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn ignores_tmp_and_malformed_files() {
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();

        // Save a real snapshot
        let real = make_snapshot(50, 1, b"real".to_vec());
        store.save(&real).await.unwrap();

        // Create debris files that should be ignored
        fs::write(store.snapshot_dir.join("100-2.snap.tmp"), b"temp junk").await.unwrap();
        fs::write(store.snapshot_dir.join("not-a-snapshot.txt"), b"junk").await.unwrap();
        fs::write(store.snapshot_dir.join("malformed.snap"), b"junk").await.unwrap();
        fs::write(store.snapshot_dir.join("abc-def.snap"), b"junk").await.unwrap();

        let latest = store.load_latest().await.unwrap().expect("should find real snapshot");
        assert_eq!(latest.metadata.last_included_offset, 50);
    }

    #[tokio::test]
    async fn begin_receive_and_finalize() {
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();

        // Build the data that the leader would serve via read_chunk
        let snapshot = make_snapshot(999, 10, b"received from leader".to_vec());
        let encoded = FileSnapshotStore::encode(&snapshot).unwrap();

        let id = SnapshotId {
            end_offset: 999,
            epoch: Term(10),
        };

        // Simulate receiving the snapshot in 256-byte chunks
        let mut writer = store.begin_receive(&id).await.unwrap();
        for chunk in encoded.chunks(256) {
            writer.write_chunk(chunk).await.unwrap();
        }
        writer.finalize().await.unwrap();

        // The snapshot should now be loadable
        let loaded = store.load_latest().await.unwrap().expect("should find received snapshot");
        assert_eq!(loaded, snapshot);
    }

    #[tokio::test]
    async fn interrupted_receive_leaves_no_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::new_v4());
        let store = FileSnapshotStore::new(tmp.path(), &cluster_id).await.unwrap();

        let id = SnapshotId {
            end_offset: 42,
            epoch: Term(1),
        };

        // Start receiving but drop the writer before finalizing
        let mut writer = store.begin_receive(&id).await.unwrap();
        writer.write_chunk(b"partial data").await.unwrap();
        drop(writer);

        // No committed snapshot should be visible
        let loaded = store.load_latest().await.unwrap();
        assert!(loaded.is_none(), "interrupted receive should not produce a visible snapshot");
    }

    #[tokio::test]
    async fn encode_decode_round_trip() {
        let snapshot = make_snapshot(777, 8, b"round trip test payload".to_vec());
        let encoded = FileSnapshotStore::encode(&snapshot).unwrap();
        let decoded = FileSnapshotStore::decode(&encoded).unwrap();
        assert_eq!(decoded, snapshot);
    }

    #[tokio::test]
    async fn decode_rejects_corrupt_data() {
        let snapshot = make_snapshot(1, 1, b"data".to_vec());
        let mut encoded = FileSnapshotStore::encode(&snapshot).unwrap();
        // Corrupt a byte in the payload region
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let result = FileSnapshotStore::decode(&encoded);
        assert!(result.is_err(), "corrupt snapshot should be rejected");
    }
}
