use std::io;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;

use xraft_core::app_record::{AppRecord, AppSnapshot};
use xraft_core::listener::Listener;
use xraft_core::log_entry::LogEntry;
use xraft_core::quorum_state::QuorumState;
use xraft_core::rpc::{RpcEnvelope, SnapshotId};
use xraft_core::snapshot::{Snapshot, SnapshotReader, SnapshotWriter};
use xraft_core::traits::{LogStore, QuorumStateStore, SnapshotIO, StateMachine, TransportSender};
use xraft_core::types::{NodeId, Term};

// ── In-memory LogStore ──────────────────────────────────────────────

pub struct InMemoryLogStore {
    entries: Mutex<Vec<LogEntry>>,
}

impl InMemoryLogStore {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn entries(&self) -> Vec<LogEntry> {
        self.entries.lock().unwrap().clone()
    }
}

impl Default for InMemoryLogStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LogStore for InMemoryLogStore {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), io::Error> {
        let mut store = self.entries.lock().unwrap();
        store.extend_from_slice(entries);
        Ok(())
    }

    async fn read(&self, start: u64, end: u64) -> Result<Vec<LogEntry>, io::Error> {
        let store = self.entries.lock().unwrap();
        Ok(store
            .iter()
            .filter(|e| e.offset >= start && e.offset < end)
            .cloned()
            .collect())
    }

    async fn truncate_suffix(&self, from: u64) -> Result<(), io::Error> {
        let mut store = self.entries.lock().unwrap();
        store.retain(|e| e.offset < from);
        Ok(())
    }

    async fn truncate_prefix(&self, up_to: u64) -> Result<(), io::Error> {
        let mut store = self.entries.lock().unwrap();
        store.retain(|e| e.offset >= up_to);
        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        let store = self.entries.lock().unwrap();
        store.first().map(|e| e.offset).unwrap_or(0)
    }

    fn log_end_offset(&self) -> u64 {
        let store = self.entries.lock().unwrap();
        store.last().map(|e| e.offset + 1).unwrap_or(0)
    }

    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>, io::Error> {
        let store = self.entries.lock().unwrap();
        Ok(store.iter().find(|e| e.offset == offset).cloned())
    }
}

// ── In-memory QuorumStateStore ──────────────────────────────────────

pub struct InMemoryQuorumStateStore {
    state: Mutex<Option<QuorumState>>,
}

impl InMemoryQuorumStateStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
    }
}

impl Default for InMemoryQuorumStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuorumStateStore for InMemoryQuorumStateStore {
    async fn load(&self) -> Result<Option<QuorumState>, io::Error> {
        Ok(self.state.lock().unwrap().clone())
    }

    async fn save(&self, state: &QuorumState) -> Result<(), io::Error> {
        *self.state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}

// ── Null TransportSender ────────────────────────────────────────────

pub struct NullTransport;

#[async_trait]
impl TransportSender for NullTransport {
    async fn send(&self, _target: NodeId, _msg: RpcEnvelope) -> Result<(), io::Error> {
        Ok(())
    }
}

// ── Null SnapshotIO ─────────────────────────────────────────────────

pub struct NullSnapshotIO;

#[async_trait]
impl SnapshotIO for NullSnapshotIO {
    async fn save(&self, _snapshot: &Snapshot) -> Result<(), io::Error> {
        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<Snapshot>, io::Error> {
        Ok(None)
    }

    async fn read_chunk(
        &self,
        _id: &SnapshotId,
        _position: u64,
        _max_bytes: u32,
    ) -> Result<(Bytes, bool), io::Error> {
        Ok((Bytes::new(), true))
    }

    async fn begin_receive(&self, _id: &SnapshotId) -> Result<SnapshotWriter, io::Error> {
        Ok(SnapshotWriter { data: Vec::new() })
    }
}

// ── Null StateMachine ───────────────────────────────────────────────

pub struct NullStateMachine;

impl StateMachine for NullStateMachine {
    fn apply(&mut self, _offset: u64, _record: &AppRecord) -> Result<(), io::Error> {
        Ok(())
    }

    fn snapshot(&self) -> Result<AppSnapshot, io::Error> {
        Ok(AppSnapshot {
            data: Vec::new(),
        })
    }

    fn restore(&mut self, _snapshot: AppSnapshot) -> Result<(), io::Error> {
        Ok(())
    }
}

// ── Recording Listener ──────────────────────────────────────────────

/// Records all Listener callbacks for test assertions.
#[derive(Clone)]
pub struct RecordingListener {
    inner: Arc<Mutex<RecordingListenerInner>>,
}

#[derive(Default)]
struct RecordingListenerInner {
    leader_changes: Vec<(NodeId, Term)>,
    shutdown_called: bool,
}

impl RecordingListener {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RecordingListenerInner::default())),
        }
    }

    pub fn leader_changes(&self) -> Vec<(NodeId, Term)> {
        self.inner.lock().unwrap().leader_changes.clone()
    }

    pub fn shutdown_called(&self) -> bool {
        self.inner.lock().unwrap().shutdown_called
    }
}

impl Default for RecordingListener {
    fn default() -> Self {
        Self::new()
    }
}

impl Listener for RecordingListener {
    fn handle_commit(&mut self, _batch: &[(u64, AppRecord)]) {}

    fn handle_load_snapshot(&mut self, _reader: SnapshotReader) {}

    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term) {
        self.inner
            .lock()
            .unwrap()
            .leader_changes
            .push((leader_id, term));
    }

    fn begin_shutdown(&mut self) {
        self.inner.lock().unwrap().shutdown_called = true;
    }
}
