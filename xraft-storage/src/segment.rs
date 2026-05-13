use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use xraft_core::{LogEntry, XraftError};

/// Magic bytes written at the start of each batch.
const BATCH_MAGIC: [u8; 4] = [0x58, 0x52, 0x41, 0x46]; // "XRAF"

/// A single log segment file covering a contiguous range of offsets.
///
/// On-disk format per batch:
///   [4 bytes magic] [4 bytes CRC32C of payload] [4 bytes payload length] [payload bytes]
///
/// Payload is `bincode`-encoded `Vec<LogEntry>`.
pub struct Segment {
    path: PathBuf,
    base_offset: u64,
    file: Mutex<File>,
    /// File size (bytes written so far).
    size: Mutex<u64>,
    /// Number of entries in this segment.
    entry_count: Mutex<u64>,
    /// Next offset to be written (base_offset + entry_count).
    next_offset: Mutex<u64>,
}

impl Segment {
    /// Open or create a segment at the given path with the given base offset.
    pub async fn open(path: &Path, base_offset: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;

        let metadata = file.metadata().await?;
        let file_size = metadata.len();

        let seg = Self {
            path: path.to_path_buf(),
            base_offset,
            file: Mutex::new(file),
            size: Mutex::new(file_size),
            entry_count: Mutex::new(0),
            next_offset: Mutex::new(base_offset),
        };

        // Recovery: scan existing batches to count entries
        if file_size > 0 {
            seg.recover().await?;
        }

        Ok(seg)
    }

    /// Recover segment state by scanning all batches.
    /// Truncates at first corruption.
    async fn recover(&self) -> io::Result<()> {
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0)).await?;

        let mut pos: u64 = 0;
        let file_size = *self.size.lock().await;
        let mut total_entries: u64 = 0;
        let mut last_good_pos: u64 = 0;

        while pos + 12 <= file_size {
            // Read batch header
            let mut header = [0u8; 12];
            if file.read_exact(&mut header).await.is_err() {
                break;
            }

            // Validate magic
            if header[0..4] != BATCH_MAGIC {
                break;
            }

            let stored_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            let payload_len =
                u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

            if pos + 12 + payload_len as u64 > file_size {
                break;
            }

            let mut payload = vec![0u8; payload_len];
            if file.read_exact(&mut payload).await.is_err() {
                break;
            }

            let computed_crc = crc32c::crc32c(&payload);
            if computed_crc != stored_crc {
                break;
            }

            let entries: Vec<LogEntry> = match bincode::deserialize(&payload) {
                Ok(e) => e,
                Err(_) => break,
            };

            total_entries += entries.len() as u64;
            pos += 12 + payload_len as u64;
            last_good_pos = pos;
        }

        // Truncate at last good position if file is corrupted
        if last_good_pos < file_size {
            file.set_len(last_good_pos).await?;
            *self.size.lock().await = last_good_pos;
        }

        *self.entry_count.lock().await = total_entries;
        *self.next_offset.lock().await = self.base_offset + total_entries;

        Ok(())
    }

    /// Append a batch of entries. Entries must have sequential offsets
    /// starting at `self.next_offset`.
    pub async fn append(&self, entries: &[LogEntry]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let payload =
            bincode::serialize(&entries.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let crc = crc32c::crc32c(&payload);

        let mut buf = Vec::with_capacity(12 + payload.len());
        buf.extend_from_slice(&BATCH_MAGIC);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::End(0)).await?;
        file.write_all(&buf).await?;
        file.flush().await?;
        file.sync_all().await?;

        *self.size.lock().await += buf.len() as u64;
        *self.entry_count.lock().await += entries.len() as u64;
        *self.next_offset.lock().await += entries.len() as u64;

        Ok(())
    }

    /// Read all entries from this segment in [start_offset, end_offset).
    pub async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>, XraftError> {
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0)).await?;

        let file_size = *self.size.lock().await;
        let mut result = Vec::new();
        let mut pos: u64 = 0;

        while pos + 12 <= file_size {
            let mut header = [0u8; 12];
            file.read_exact(&mut header).await?;

            if header[0..4] != BATCH_MAGIC {
                return Err(XraftError::Corruption("invalid batch magic".into()));
            }

            let stored_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            let payload_len =
                u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

            let mut payload = vec![0u8; payload_len];
            file.read_exact(&mut payload).await?;

            let computed_crc = crc32c::crc32c(&payload);
            if computed_crc != stored_crc {
                return Err(XraftError::Corruption("CRC mismatch".into()));
            }

            let entries: Vec<LogEntry> = bincode::deserialize(&payload)
                .map_err(|e| XraftError::Corruption(format!("deserialize error: {e}")))?;

            for entry in entries {
                if entry.offset >= start_offset && entry.offset < end_offset {
                    result.push(entry);
                }
            }

            pos += 12 + payload_len as u64;
        }

        Ok(result)
    }

    /// Read all entries in this segment.
    pub async fn read_all(&self) -> Result<Vec<LogEntry>, XraftError> {
        self.read(0, u64::MAX).await
    }

    /// Truncate all entries at and after `from_offset`.
    pub async fn truncate_suffix(&self, from_offset: u64) -> io::Result<bool> {
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0)).await?;

        let file_size = *self.size.lock().await;
        let mut pos: u64 = 0;
        let mut truncate_pos: Option<u64> = None;
        let mut remaining_entries: u64 = 0;

        while pos + 12 <= file_size {
            let batch_start = pos;
            let mut header = [0u8; 12];
            file.read_exact(&mut header).await?;

            if header[0..4] != BATCH_MAGIC {
                break;
            }

            let payload_len =
                u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

            let mut payload = vec![0u8; payload_len];
            file.read_exact(&mut payload).await?;

            let entries: Vec<LogEntry> = match bincode::deserialize(&payload) {
                Ok(e) => e,
                Err(_) => break,
            };

            // If any entry in this batch is at or after from_offset, truncate here
            let batch_has_target = entries.iter().any(|e| e.offset >= from_offset);
            if batch_has_target {
                // We need to check if partial batch truncation is needed
                let entries_before: Vec<LogEntry> =
                    entries.into_iter().filter(|e| e.offset < from_offset).collect();
                if entries_before.is_empty() {
                    truncate_pos = Some(batch_start);
                } else {
                    // Rewrite this batch with only the entries before from_offset
                    remaining_entries += entries_before.len() as u64;
                    let new_payload = bincode::serialize(&entries_before)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    let new_crc = crc32c::crc32c(&new_payload);

                    file.seek(SeekFrom::Start(batch_start)).await?;
                    file.write_all(&BATCH_MAGIC).await?;
                    file.write_all(&new_crc.to_le_bytes()).await?;
                    file.write_all(&(new_payload.len() as u32).to_le_bytes()).await?;
                    file.write_all(&new_payload).await?;

                    truncate_pos = Some(batch_start + 12 + new_payload.len() as u64);
                }
                break;
            }

            remaining_entries += entries.len() as u64;
            pos += 12 + payload_len as u64;
        }

        if let Some(trunc_pos) = truncate_pos {
            file.set_len(trunc_pos).await?;
            file.sync_all().await?;
            *self.size.lock().await = trunc_pos;
            *self.entry_count.lock().await = remaining_entries;
            *self.next_offset.lock().await = self.base_offset + remaining_entries;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub async fn next_offset(&self) -> u64 {
        *self.next_offset.lock().await
    }

    pub async fn entry_count(&self) -> u64 {
        *self.entry_count.lock().await
    }

    pub async fn file_size(&self) -> u64 {
        *self.size.lock().await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
