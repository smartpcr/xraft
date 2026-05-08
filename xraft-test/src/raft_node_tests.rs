use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, Instant};

use xraft_core::app_record::{AppRecord, AppSnapshot};
use xraft_core::config::RaftConfig;
use xraft_core::consensus_state::Role;
use xraft_core::error::{XraftError, XraftResult};
use xraft_core::listener::Listener;
use xraft_core::log_entry::LogEntry;
use xraft_core::quorum_state::QuorumState;
use xraft_core::rpc::{RpcEnvelope, SnapshotId};
use xraft_core::snapshot::{Snapshot, SnapshotMetadata, SnapshotWriter};
use xraft_core::traits::{
    Clock, LogStore, QuorumStateStore, SnapshotIO, StateMachine, TransportReceiver,
    TransportSender,
};
use xraft_core::types::{NodeId, Term};
use xraft_core::voter::VoterInfo;
use xraft_core::RaftNode;

// ── Mock implementations ──────────────────────────────────────────

struct MockLogStore {
    end_offset: AtomicU64,
    entries: Mutex<Vec<LogEntry>>,
}

impl MockLogStore {
    fn new() -> Self {
        Self {
            end_offset: AtomicU64::new(0),
            entries: Mutex::new(Vec::new()),
        }
    }

    fn with_end_offset(offset: u64) -> Self {
        Self {
            end_offset: AtomicU64::new(offset),
            entries: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl LogStore for MockLogStore {
    async fn append(&self, entries: &[LogEntry]) -> XraftResult<()> {
        let mut stored = self.entries.lock().unwrap();
        for e in entries {
            stored.push(e.clone());
        }
        self.end_offset
            .fetch_add(entries.len() as u64, Ordering::SeqCst);
        Ok(())
    }
    async fn read(&self, _start: u64, _end: u64) -> XraftResult<Vec<LogEntry>> {
        Ok(Vec::new())
    }
    async fn truncate_suffix(&self, _from: u64) -> XraftResult<()> {
        Ok(())
    }
    async fn truncate_prefix(&self, _up_to: u64) -> XraftResult<()> {
        Ok(())
    }
    fn log_start_offset(&self) -> u64 {
        0
    }
    fn log_end_offset(&self) -> u64 {
        self.end_offset.load(Ordering::SeqCst)
    }
    async fn entry_at(&self, _offset: u64) -> XraftResult<Option<LogEntry>> {
        Ok(None)
    }
}

struct MockQuorumStateStore {
    state: Mutex<Option<QuorumState>>,
    save_called: AtomicBool,
}

impl MockQuorumStateStore {
    fn empty() -> Self {
        Self {
            state: Mutex::new(None),
            save_called: AtomicBool::new(false),
        }
    }

    fn with_existing_state() -> Self {
        Self {
            state: Mutex::new(Some(QuorumState {
                current_term: Term(1),
                voted_for: None,
                leader_id: None,
                leader_epoch: Term(0),
            })),
            save_called: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl QuorumStateStore for MockQuorumStateStore {
    async fn load(&self) -> XraftResult<Option<QuorumState>> {
        Ok(self.state.lock().unwrap().clone())
    }
    async fn save(&self, state: &QuorumState) -> XraftResult<()> {
        *self.state.lock().unwrap() = Some(state.clone());
        self.save_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct MockSnapshotIO {
    has_snapshot: bool,
}

impl MockSnapshotIO {
    fn empty() -> Self {
        Self {
            has_snapshot: false,
        }
    }

    fn with_existing_snapshot() -> Self {
        Self {
            has_snapshot: true,
        }
    }
}

#[async_trait]
impl SnapshotIO for MockSnapshotIO {
    async fn save(&self, _snapshot: &Snapshot) -> XraftResult<()> {
        Ok(())
    }
    async fn load_latest(&self) -> XraftResult<Option<Snapshot>> {
        if self.has_snapshot {
            Ok(Some(Snapshot {
                metadata: SnapshotMetadata {
                    last_included_offset: 10,
                    last_included_term: Term(1),
                    voters: vec![],
                    leader_epoch: Term(1),
                },
                app_snapshot: AppSnapshot { data: vec![] },
            }))
        } else {
            Ok(None)
        }
    }
    async fn read_chunk(
        &self,
        _id: &SnapshotId,
        _position: u64,
        _max_bytes: u32,
    ) -> XraftResult<(Bytes, bool)> {
        Ok((Bytes::new(), true))
    }
    async fn begin_receive(&self, _id: &SnapshotId) -> XraftResult<SnapshotWriter> {
        Ok(SnapshotWriter { data: Vec::new() })
    }
}

struct MockTransportSender;

#[async_trait]
impl TransportSender for MockTransportSender {
    async fn send(&self, _target: NodeId, _message: RpcEnvelope) -> XraftResult<()> {
        Ok(())
    }
}

struct MockTransportReceiver;

#[async_trait]
impl TransportReceiver for MockTransportReceiver {
    async fn recv(&mut self) -> XraftResult<RpcEnvelope> {
        // Block forever — no inbound messages in tests.
        std::future::pending().await
    }
}

struct MockClock;

#[async_trait]
impl Clock for MockClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep_until(&self, deadline: Instant) {
        tokio::time::sleep_until(deadline).await;
    }
    fn random_election_timeout(&self) -> Duration {
        Duration::from_millis(200)
    }
}

struct MockStateMachine;

impl StateMachine for MockStateMachine {
    fn apply(&mut self, _offset: u64, _record: &AppRecord) -> XraftResult<()> {
        Ok(())
    }
    fn snapshot(&self) -> XraftResult<AppSnapshot> {
        Ok(AppSnapshot {
            data: Vec::new(),
        })
    }
    fn restore(&mut self, _snapshot: AppSnapshot) -> XraftResult<()> {
        Ok(())
    }
}

struct MockListener {
    shutdown_called: Arc<AtomicBool>,
}

impl MockListener {
    fn new() -> Self {
        Self {
            shutdown_called: Arc::new(AtomicBool::new(false)),
        }
    }

    fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown_called)
    }
}

impl Listener for MockListener {
    fn handle_commit(&mut self, _batch: &[(u64, AppRecord)]) {}
    fn handle_load_snapshot(&mut self, _reader: xraft_core::snapshot::SnapshotReader) {}
    fn handle_leader_change(&mut self, _leader_id: NodeId, _term: Term) {}
    fn begin_shutdown(&mut self) {
        self.shutdown_called.store(true, Ordering::SeqCst);
    }
}

// ── Helper ────────────────────────────────────────────────────────

fn make_node() -> RaftNode<MockStateMachine, MockListener> {
    RaftNode::new(
        RaftConfig::default(),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::empty()),
        Box::new(MockSnapshotIO::empty()),
        Box::new(MockTransportSender),
        Box::new(MockTransportReceiver),
        Box::new(MockClock),
        MockStateMachine,
        MockListener::new(),
    )
    .expect("RaftNode::new should succeed")
}

fn make_node_with_listener() -> (RaftNode<MockStateMachine, MockListener>, Arc<AtomicBool>) {
    let listener = MockListener::new();
    let flag = listener.shutdown_flag();
    let node = RaftNode::new(
        RaftConfig::default(),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::empty()),
        Box::new(MockSnapshotIO::empty()),
        Box::new(MockTransportSender),
        Box::new(MockTransportReceiver),
        Box::new(MockClock),
        MockStateMachine,
        listener,
    )
    .expect("RaftNode::new should succeed");
    (node, flag)
}

// ── Tests ─────────────────────────────────────────────────────────

/// Scenario: RaftNode compiles — the struct with all type parameters
/// compiles and can be constructed.
#[test]
fn raft_node_compiles_and_constructs() {
    let _node = make_node();
}

/// Scenario: read() returns initial ConsensusState.
#[test]
fn read_returns_initial_state() {
    let node = make_node();
    let state = node.read().expect("read() should succeed");
    assert_eq!(state.role, Role::Unattached);
    assert_eq!(state.high_watermark, 0);
    assert_eq!(state.log_end_offset, 0);
    assert!(state.leader_id.is_none());
    assert!(state.voter_set.is_empty());
}

/// Scenario: propose() returns NotLeader when no leader is active
/// (Phase 1.7 skeleton — initial state is Unattached with no leader).
#[tokio::test]
async fn propose_returns_not_leader_when_no_leader_active() {
    let node = make_node();
    let cmd = AppRecord {
        data: Bytes::from_static(b"hello"),
    };
    let result = node.propose(cmd).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        XraftError::NotLeader { leader_id } => {
            assert!(leader_id.is_none(), "no leader should be known");
        }
        other => panic!("expected NotLeader, got: {other}"),
    }
}

/// Scenario: propose() returns NotLeader for any non-leader role.
#[tokio::test]
async fn propose_returns_not_leader() {
    let node = make_node();
    let cmd = AppRecord {
        data: Bytes::from_static(b"hello"),
    };
    let result = node.propose(cmd).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), XraftError::NotLeader { .. }));
}

/// Scenario: bootstrap() succeeds on empty state and stores configuration in memory.
#[tokio::test]
async fn bootstrap_succeeds_on_empty_state() {
    let mut node = RaftNode::new(
        RaftConfig::default(),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::empty()),
        Box::new(MockSnapshotIO::empty()),
        Box::new(MockTransportSender),
        Box::new(MockTransportReceiver),
        Box::new(MockClock),
        MockStateMachine,
        MockListener::new(),
    )
    .expect("RaftNode::new should succeed");

    let cluster_id = xraft_core::types::ClusterId(uuid::Uuid::new_v4());
    let voters = vec![
        VoterInfo {
            node_id: NodeId(1),
            endpoint: "127.0.0.1:9000".parse().unwrap(),
        },
        VoterInfo {
            node_id: NodeId(2),
            endpoint: "127.0.0.1:9001".parse().unwrap(),
        },
    ];
    let result = node.bootstrap(cluster_id, voters.clone()).await;
    assert!(result.is_ok(), "bootstrap should succeed: {:?}", result);

    // After skeleton bootstrap, read() is unaffected (Phase 6 will update watch).
    let state = node.read().expect("read() should succeed");
    assert!(state.voter_set.is_empty());
    assert_eq!(state.log_end_offset, 0);
}

/// Scenario: bootstrap() fails when log is not empty.
#[tokio::test]
async fn bootstrap_fails_when_log_not_empty() {
    let mut node = RaftNode::new(
        RaftConfig::default(),
        Box::new(MockLogStore::with_end_offset(5)),
        Box::new(MockQuorumStateStore::empty()),
        Box::new(MockSnapshotIO::empty()),
        Box::new(MockTransportSender),
        Box::new(MockTransportReceiver),
        Box::new(MockClock),
        MockStateMachine,
        MockListener::new(),
    )
    .expect("RaftNode::new should succeed");

    let cluster_id = xraft_core::types::ClusterId(uuid::Uuid::new_v4());
    let result = node
        .bootstrap(cluster_id, vec![VoterInfo {
            node_id: NodeId(1),
            endpoint: "127.0.0.1:9000".parse().unwrap(),
        }])
        .await;

    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("log is not empty"),
        "expected 'log is not empty', got: {err_msg}"
    );
}

/// Scenario: bootstrap() fails when quorum-state already exists.
#[tokio::test]
async fn bootstrap_fails_when_quorum_state_exists() {
    let mut node = RaftNode::new(
        RaftConfig::default(),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::with_existing_state()),
        Box::new(MockSnapshotIO::empty()),
        Box::new(MockTransportSender),
        Box::new(MockTransportReceiver),
        Box::new(MockClock),
        MockStateMachine,
        MockListener::new(),
    )
    .expect("RaftNode::new should succeed");

    let cluster_id = xraft_core::types::ClusterId(uuid::Uuid::new_v4());
    let voters = vec![VoterInfo {
        node_id: NodeId(1),
        endpoint: "127.0.0.1:9000".parse().unwrap(),
    }];
    let result = node.bootstrap(cluster_id, voters).await;

    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("quorum-state already exists"),
        "expected 'quorum-state already exists', got: {err_msg}"
    );
}

/// Scenario: bootstrap() fails when snapshot already exists.
#[tokio::test]
async fn bootstrap_fails_when_snapshot_exists() {
    let mut node = RaftNode::new(
        RaftConfig::default(),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::empty()),
        Box::new(MockSnapshotIO::with_existing_snapshot()),
        Box::new(MockTransportSender),
        Box::new(MockTransportReceiver),
        Box::new(MockClock),
        MockStateMachine,
        MockListener::new(),
    )
    .expect("RaftNode::new should succeed");

    let cluster_id = xraft_core::types::ClusterId(uuid::Uuid::new_v4());
    let voters = vec![VoterInfo {
        node_id: NodeId(1),
        endpoint: "127.0.0.1:9000".parse().unwrap(),
    }];
    let result = node.bootstrap(cluster_id, voters).await;

    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("snapshot already exists"),
        "expected 'snapshot already exists', got: {err_msg}"
    );
}

/// Scenario: shutdown() invokes begin_shutdown on the listener (Phase 1.7
/// transitional — called directly since no event loop is running).
#[tokio::test]
async fn shutdown_invokes_begin_shutdown() {
    let (node, shutdown_flag) = make_node_with_listener();
    assert!(!shutdown_flag.load(Ordering::SeqCst));

    let result = node.shutdown().await;
    assert!(result.is_ok());
    assert!(
        shutdown_flag.load(Ordering::SeqCst),
        "begin_shutdown() should have been called"
    );
}

/// Scenario: shutdown() succeeds even with no tasks running (Phase 1.7).
#[tokio::test]
async fn shutdown_succeeds_with_no_tasks() {
    let node = make_node();
    let result = node.shutdown().await;
    assert!(result.is_ok());
}

/// Scenario: API surface — all methods return typed Results.
#[tokio::test]
async fn api_surface_returns_typed_results() {
    let mut node = make_node();

    // read() -> Result<ConsensusState>
    let _state: XraftResult<xraft_core::consensus_state::ConsensusState> = node.read();

    // propose() -> Result<Offset>
    let cmd = AppRecord {
        data: Bytes::from_static(b"cmd"),
    };
    let _propose: XraftResult<xraft_core::types::Offset> = node.propose(cmd).await;

    // bootstrap() -> Result<()>
    let cluster_id = xraft_core::types::ClusterId(uuid::Uuid::new_v4());
    let _bootstrap: XraftResult<()> = node.bootstrap(cluster_id, vec![]).await;

    // shutdown() -> Result<()>
    let _shutdown: XraftResult<()> = node.shutdown().await;
}

/// Scenario: bootstrap stores configuration in memory for Phase 6 persistence.
#[tokio::test]
async fn bootstrap_stores_configuration_in_memory() {
    let mut node = make_node();
    let cluster_id = xraft_core::types::ClusterId(uuid::Uuid::new_v4());
    let voters = vec![VoterInfo {
        node_id: NodeId(1),
        endpoint: "127.0.0.1:9000".parse().unwrap(),
    }];

    node.bootstrap(cluster_id, voters).await.expect("bootstrap should succeed");

    // Skeleton bootstrap stores in memory only — read() unaffected.
    let state = node.read().expect("read should succeed");
    assert!(state.voter_set.is_empty());
    assert_eq!(state.log_end_offset, 0);
}

/// Scenario: new() fails with invalid config.
#[test]
fn new_fails_with_invalid_config() {
    let bad_config = RaftConfig {
        election_timeout_min_ms: 300,
        election_timeout_max_ms: 150, // invalid: min > max
        ..RaftConfig::default()
    };
    let result = RaftNode::new(
        bad_config,
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::empty()),
        Box::new(MockSnapshotIO::empty()),
        Box::new(MockTransportSender),
        Box::new(MockTransportReceiver),
        Box::new(MockClock),
        MockStateMachine,
        MockListener::new(),
    );
    assert!(result.is_err());
}
