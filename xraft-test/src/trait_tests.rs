use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

use xraft_core::app_record::{AppRecord, AppSnapshot};
use xraft_core::log_entry::{EntryType, LogEntry};
use xraft_core::quorum_state::QuorumState;
use xraft_core::rpc::{RpcEnvelope, RpcPayload, VoteRequest};
use xraft_core::snapshot::{Snapshot, SnapshotId, SnapshotMetadata, SnapshotWriter};
use xraft_core::traits::{
    Clock, LogStore, QuorumStateStore, Result, SnapshotIO, StateMachine, TransportReceiver,
    TransportSender,
};
use xraft_core::types::{ClusterId, NodeId, Term};

// ---------------------------------------------------------------------------
// Mock LogStore
// ---------------------------------------------------------------------------

struct MockLogStore {
    start: AtomicU64,
    end: AtomicU64,
}

impl MockLogStore {
    fn new() -> Self {
        Self {
            start: AtomicU64::new(0),
            end: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl LogStore for MockLogStore {
    async fn append(&self, entries: &[LogEntry]) -> Result<()> {
        self.end
            .fetch_add(entries.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self, _start: u64, _end: u64) -> Result<Vec<LogEntry>> {
        Ok(vec![])
    }

    async fn truncate_suffix(&self, from_offset: u64) -> Result<()> {
        self.end.store(from_offset, Ordering::SeqCst);
        Ok(())
    }

    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()> {
        self.start.store(up_to_offset, Ordering::SeqCst);
        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        self.start.load(Ordering::SeqCst)
    }

    fn log_end_offset(&self) -> u64 {
        self.end.load(Ordering::SeqCst)
    }

    async fn entry_at(&self, _offset: u64) -> Result<Option<LogEntry>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Simulated Clock
// ---------------------------------------------------------------------------

struct SimulatedClock;

#[async_trait]
impl Clock for SimulatedClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep_until(&self, deadline: Instant) {
        let now = Instant::now();
        if deadline > now {
            tokio::time::sleep(deadline - now).await;
        }
    }

    fn random_election_timeout(&self) -> Duration {
        Duration::from_millis(150)
    }
}

// ---------------------------------------------------------------------------
// Dummy StateMachine
// ---------------------------------------------------------------------------

struct DummyStateMachine {
    applied: Vec<u64>,
}

impl DummyStateMachine {
    fn new() -> Self {
        Self { applied: vec![] }
    }
}

impl StateMachine for DummyStateMachine {
    fn apply(&mut self, offset: u64, _record: &AppRecord) -> Result<()> {
        self.applied.push(offset);
        Ok(())
    }

    fn snapshot(&self) -> Result<AppSnapshot> {
        Ok(AppSnapshot {
            data: self.applied.iter().flat_map(|o| o.to_le_bytes()).collect(),
        })
    }

    fn restore(&mut self, _snapshot: AppSnapshot) -> Result<()> {
        self.applied.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// LogStore is object-safe: `Box<dyn LogStore>` compiles and works.
#[tokio::test]
async fn log_store_object_safety() {
    let store: Box<dyn LogStore> = Box::new(MockLogStore::new());

    let entry = LogEntry {
        offset: 0,
        term: Term(1),
        entry_type: EntryType::Command,
        payload: Bytes::from_static(b"hello"),
    };

    store.append(&[entry]).await.unwrap();
    assert_eq!(store.log_end_offset(), 1);
    assert_eq!(store.log_start_offset(), 0);

    let read = store.read(0, 1).await.unwrap();
    assert!(read.is_empty()); // mock returns empty

    let at = store.entry_at(0).await.unwrap();
    assert!(at.is_none());

    store.truncate_suffix(0).await.unwrap();
    assert_eq!(store.log_end_offset(), 0);

    store.truncate_prefix(5).await.unwrap();
    assert_eq!(store.log_start_offset(), 5);
}

/// Clock is object-safe: `Box<dyn Clock>` compiles and `sleep_until` works
/// via dynamic dispatch.
#[tokio::test]
async fn clock_object_safety() {
    let clock: Box<dyn Clock> = Box::new(SimulatedClock);

    let now = clock.now();
    let timeout = clock.random_election_timeout();
    assert!(timeout.as_millis() > 0);

    // sleep_until a deadline in the past should return immediately
    clock.sleep_until(now).await;
}

/// StateMachine apply works with AppRecord.
#[test]
fn state_machine_apply() {
    let mut sm = DummyStateMachine::new();
    let record = AppRecord {
        data: Bytes::from_static(b"cmd"),
    };
    sm.apply(0, &record).unwrap();
    sm.apply(1, &record).unwrap();
    assert_eq!(sm.applied, vec![0, 1]);

    let snap = sm.snapshot().unwrap();
    assert!(!snap.data.is_empty());

    sm.restore(snap).unwrap();
    assert!(sm.applied.is_empty());
}

/// TransportSender is object-safe.
#[tokio::test]
async fn transport_sender_object_safety() {
    struct NoopSender;

    #[async_trait]
    impl TransportSender for NoopSender {
        async fn send(&self, _target: NodeId, _message: RpcEnvelope) -> Result<()> {
            Ok(())
        }
    }

    let sender: Box<dyn TransportSender> = Box::new(NoopSender);
    let envelope = RpcEnvelope {
        cluster_id: ClusterId(uuid::Uuid::nil()),
        leader_epoch: Term(1),
        source: NodeId(1),
        payload: RpcPayload::VoteRequest(VoteRequest {
            term: Term(1),
            candidate_id: NodeId(1),
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: false,
        }),
    };
    sender.send(NodeId(2), envelope).await.unwrap();
}

/// TransportReceiver is object-safe (as `Box<dyn TransportReceiver>`).
#[tokio::test]
async fn transport_receiver_object_safety() {
    struct OneShot {
        sent: bool,
    }

    #[async_trait]
    impl TransportReceiver for OneShot {
        async fn recv(&mut self) -> Result<RpcEnvelope> {
            if self.sent {
                return Err("no more".into());
            }
            self.sent = true;
            Ok(RpcEnvelope {
                cluster_id: ClusterId(uuid::Uuid::nil()),
                leader_epoch: Term(1),
                source: NodeId(2),
                payload: RpcPayload::VoteRequest(VoteRequest {
                    term: Term(1),
                    candidate_id: NodeId(2),
                    last_log_offset: 0,
                    last_log_term: Term(0),
                    is_pre_vote: true,
                }),
            })
        }
    }

    let mut rx: Box<dyn TransportReceiver> = Box::new(OneShot { sent: false });
    let msg = rx.recv().await.unwrap();
    assert_eq!(msg.source, NodeId(2));
    assert!(rx.recv().await.is_err());
}

/// QuorumStateStore is object-safe.
#[tokio::test]
async fn quorum_state_store_object_safety() {
    struct InMemoryQS {
        state: tokio::sync::Mutex<Option<QuorumState>>,
    }

    #[async_trait]
    impl QuorumStateStore for InMemoryQS {
        async fn load(&self) -> Result<Option<QuorumState>> {
            Ok(self.state.lock().await.clone())
        }
        async fn save(&self, state: &QuorumState) -> Result<()> {
            *self.state.lock().await = Some(state.clone());
            Ok(())
        }
    }

    let store: Box<dyn QuorumStateStore> = Box::new(InMemoryQS {
        state: tokio::sync::Mutex::new(None),
    });

    assert!(store.load().await.unwrap().is_none());

    let qs = QuorumState {
        current_term: Term(3),
        voted_for: Some(NodeId(1)),
        leader_id: None,
        leader_epoch: Term(2),
    };
    store.save(&qs).await.unwrap();
    let loaded = store.load().await.unwrap().unwrap();
    assert_eq!(loaded.current_term, Term(3));
}

/// SnapshotIO is object-safe.
#[tokio::test]
async fn snapshot_io_object_safety() {
    struct NoopSnapshotIO;

    #[async_trait]
    impl SnapshotIO for NoopSnapshotIO {
        async fn save(&self, _snapshot: &Snapshot) -> Result<()> {
            Ok(())
        }
        async fn load_latest(&self) -> Result<Option<Snapshot>> {
            Ok(None)
        }
        async fn read_chunk(
            &self,
            _id: &SnapshotId,
            _position: u64,
            _max_bytes: u32,
        ) -> Result<(Bytes, bool)> {
            Ok((Bytes::new(), true))
        }
        async fn begin_receive(&self, _id: &SnapshotId) -> Result<SnapshotWriter> {
            Ok(SnapshotWriter::new())
        }
    }

    let io: Box<dyn SnapshotIO> = Box::new(NoopSnapshotIO);
    assert!(io.load_latest().await.unwrap().is_none());

    let snap = Snapshot {
        metadata: SnapshotMetadata {
            last_included_offset: 10,
            last_included_term: Term(2),
            voters: vec![],
            leader_epoch: Term(2),
        },
        app_snapshot: AppSnapshot { data: vec![1, 2, 3] },
    };
    io.save(&snap).await.unwrap();

    let id = SnapshotId {
        end_offset: 10,
        epoch: Term(2),
    };
    let (chunk, done) = io.read_chunk(&id, 0, 1024).await.unwrap();
    assert!(chunk.is_empty());
    assert!(done);

    let _writer = io.begin_receive(&id).await.unwrap();
}
