use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::app_record::AppRecord;
use crate::config::RaftConfig;
use crate::consensus_state::{ConsensusState, Role};
use crate::error::{XraftError, XraftResult};
use crate::listener::Listener;
use crate::traits::{
    Clock, LogStore, QuorumStateStore, SnapshotIO, StateMachine, TransportReceiver,
    TransportSender,
};
use crate::types::{ClusterId, Offset, Term};
use crate::voter::VoterInfo;

/// Internal message sent to the event loop via the mpsc channel.
pub(crate) enum EventLoopMsg {
    /// A client proposal carrying an `AppRecord` and a oneshot channel
    /// for returning the assigned `Offset` once committed.
    #[allow(dead_code)]
    Propose {
        record: AppRecord,
        reply: tokio::sync::oneshot::Sender<XraftResult<Offset>>,
    },
    /// An inbound RPC envelope forwarded by the ReceiverTask.
    #[allow(dead_code)]
    Inbound(crate::rpc::RpcEnvelope),
}

/// Public entry point for an xraft consensus node.
///
/// Generic over the application-provided `StateMachine` (`S`) and `Listener`
/// (`L`) — monomorphised at compile time (architecture §4.1). I/O traits
/// (`LogStore`, `TransportSender`, `TransportReceiver`, `QuorumStateStore`,
/// `SnapshotIO`) and the `Clock` runtime trait are injected as `Box<dyn ...>`
/// trait objects at construction time.
///
/// # Lifecycle
///
/// 1. **Construction** (`new`) — creates channels, stores all injected
///    components. Does **not** start the event loop (Phase 4) or run
///    recovery (Phase 6). Safe to call outside a Tokio runtime.
/// 2. **Bootstrap** (`bootstrap`) — validates preconditions (empty log,
///    no quorum-state file, no snapshot) and stores initial cluster
///    configuration in memory. Full persistence is deferred to Phase 6.
/// 3. **Propose / Read** — `propose()` returns `NotLeader` in the initial
///    Unattached state; `read()` returns the latest `ConsensusState`
///    from the watch channel.
/// 4. **Shutdown** (`shutdown`) — invokes `Listener::begin_shutdown()`
///    synchronously, drops the mpsc sender (causing the ReceiverTask to
///    exit per architecture §4.4 once started in Phase 4), and awaits
///    both task handles if they exist.
pub struct RaftNode<S: StateMachine, L: Listener> {
    #[allow(dead_code)]
    config: RaftConfig,

    /// Handle for the event loop task. `None` until the event loop is
    /// started in Phase 4.
    #[allow(dead_code)]
    event_loop_handle: Option<JoinHandle<()>>,

    /// Handle for the receiver task. `None` until the ReceiverTask is
    /// started in Phase 4.
    #[allow(dead_code)]
    receiver_task_handle: Option<JoinHandle<()>>,

    /// Sender half of the mpsc channel used by `propose()` and (in
    /// Phase 4) the ReceiverTask to feed messages into the event loop.
    propose_tx: mpsc::Sender<EventLoopMsg>,

    /// Receiver half of the mpsc channel. Held here until Phase 4
    /// when it is passed to the EventLoop.
    #[allow(dead_code)]
    event_loop_rx: Option<mpsc::Receiver<EventLoopMsg>>,

    /// Watch sender for `ConsensusState`. The event loop is the sole
    /// writer (Phase 4). Held here until then.
    #[allow(dead_code)]
    state_tx: watch::Sender<ConsensusState>,

    /// Watch receiver for the latest `ConsensusState`. `read()` clones
    /// the current value.
    state_rx: watch::Receiver<ConsensusState>,

    // --- I/O trait objects held for Phase 4 ---

    #[allow(dead_code)]
    log_store: Box<dyn LogStore>,
    #[allow(dead_code)]
    quorum_state_store: Box<dyn QuorumStateStore>,
    #[allow(dead_code)]
    snapshot_io: Box<dyn SnapshotIO>,
    #[allow(dead_code)]
    transport_sender: Box<dyn TransportSender>,
    #[allow(dead_code)]
    transport_receiver: Option<Box<dyn TransportReceiver>>,
    #[allow(dead_code)]
    clock: Box<dyn Clock>,

    // --- Application-provided types ---

    #[allow(dead_code)]
    state_machine: Option<S>,
    listener: Option<L>,

    /// Set by `bootstrap()` — stored in memory until Phase 6 persistence.
    #[allow(dead_code)]
    pub(crate) cluster_id: Option<ClusterId>,
    /// Set by `bootstrap()` — stored in memory until Phase 6 persistence.
    #[allow(dead_code)]
    pub(crate) initial_voters: Option<Vec<VoterInfo>>,

    /// Guard against calling bootstrap more than once.
    bootstrapped: bool,
}

impl<S: StateMachine, L: Listener> RaftNode<S, L> {
    /// Construct a new `RaftNode`.
    ///
    /// Accepts I/O trait objects (separate `Box<dyn TransportSender>` and
    /// `Box<dyn TransportReceiver>` — callers use `TcpTransport::split()`
    /// or `ChannelTransport::split()` to obtain the halves per architecture
    /// §4.4), the `Clock` runtime trait object (`Box<dyn Clock>` — passed
    /// to the `EventLoop` for timer management, not the `IoStage`), and
    /// application-provided `S` / `L` instances.
    ///
    /// Creates the `tokio::sync::watch` channel for `ConsensusState`
    /// (writer end stored for the EventLoop, receiver end retained for
    /// `read()`). Creates the `tokio::sync::mpsc` channel (sender
    /// retained for `propose()`, receiver stored for the EventLoop).
    ///
    /// Does **not** start the event loop (started in Phase 4) or run
    /// recovery (completed in Phase 6). Safe to call outside a Tokio
    /// runtime — no tasks are spawned.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: RaftConfig,
        log_store: Box<dyn LogStore>,
        quorum_state_store: Box<dyn QuorumStateStore>,
        snapshot_io: Box<dyn SnapshotIO>,
        transport_sender: Box<dyn TransportSender>,
        transport_receiver: Box<dyn TransportReceiver>,
        clock: Box<dyn Clock>,
        state_machine: S,
        listener: L,
    ) -> XraftResult<Self> {
        config.validate()?;

        let initial_state = ConsensusState {
            current_term: Term(0),
            role: Role::Unattached,
            leader_id: None,
            high_watermark: 0,
            log_end_offset: 0,
            voter_set: Vec::new(),
            node_id: config.node_id,
        };

        let (state_tx, state_rx) = watch::channel(initial_state);
        let (propose_tx, event_loop_rx) = mpsc::channel::<EventLoopMsg>(256);

        Ok(Self {
            config,
            event_loop_handle: None,
            receiver_task_handle: None,
            propose_tx,
            event_loop_rx: Some(event_loop_rx),
            state_tx,
            state_rx,
            log_store,
            quorum_state_store,
            snapshot_io,
            transport_sender,
            transport_receiver: Some(transport_receiver),
            clock,
            state_machine: Some(state_machine),
            listener: Some(listener),
            cluster_id: None,
            initial_voters: None,
            bootstrapped: false,
        })
    }

    /// Propose an application command to the Raft cluster.
    ///
    /// Sends the `AppRecord` to the event loop via the internal mpsc
    /// channel and returns a future that resolves to the committed
    /// `Offset`. Returns `NotLeader` when no leader is active.
    ///
    /// # Phase 1.7 behavior
    ///
    /// The event loop is not running, so the role is always `Unattached`
    /// and this method always returns `NotLeader`. In Phase 5, the
    /// `NotLeader` check remains at the call-site for fast rejection,
    /// and the command is sent to the event loop channel for consensus
    /// processing.
    pub async fn propose(&self, command: AppRecord) -> XraftResult<Offset> {
        // Fast-path: check local watch state to reject non-leaders
        // without entering the event loop's message queue.
        let current = self.state_rx.borrow().clone();
        if current.role != Role::Leader {
            return Err(XraftError::NotLeader {
                leader_id: current.leader_id,
            });
        }

        // Send command to event loop via mpsc channel and await the
        // committed offset via oneshot reply.
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.propose_tx
            .send(EventLoopMsg::Propose {
                record: command,
                reply: reply_tx,
            })
            .await
            .map_err(|_| XraftError::Shutdown)?;

        reply_rx.await.map_err(|_| XraftError::Shutdown)?
    }

    /// Return the current committed protocol state.
    ///
    /// Clones the latest `ConsensusState` from the watch channel.
    /// Never reads the `LogStore`, enters the event loop queue, or
    /// contacts other nodes. Callable on any node role.
    ///
    /// The `high_watermark` in the returned state is an exclusive upper
    /// bound: entry at offset O is committed when O < HW (architecture §3.1).
    pub fn read(&self) -> XraftResult<ConsensusState> {
        Ok(self.state_rx.borrow().clone())
    }

    /// Bootstrap the cluster with an initial voter set.
    ///
    /// Validates preconditions (empty log, no quorum-state file, no
    /// existing snapshot, not already bootstrapped) and stores the cluster
    /// configuration in memory.
    ///
    /// `ClusterId` is provided by the caller to ensure all nodes share
    /// the same cluster identity.
    ///
    /// Full bootstrap persistence is deferred to Phase 6.
    pub async fn bootstrap(
        &mut self,
        cluster_id: ClusterId,
        initial_voters: Vec<VoterInfo>,
    ) -> XraftResult<()> {
        if self.bootstrapped {
            return Err(XraftError::BootstrapPreconditionFailed(
                "node has already been bootstrapped".into(),
            ));
        }

        // Storage preconditions first — reject if node already has durable state.
        if self.log_store.log_end_offset() != 0 {
            return Err(XraftError::BootstrapPreconditionFailed(
                "log is not empty".into(),
            ));
        }

        if self.quorum_state_store.load().await?.is_some() {
            return Err(XraftError::BootstrapPreconditionFailed(
                "quorum-state already exists".into(),
            ));
        }

        if self.snapshot_io.load_latest().await?.is_some() {
            return Err(XraftError::BootstrapPreconditionFailed(
                "snapshot already exists".into(),
            ));
        }

        // Input validation after storage preconditions.
        if initial_voters.is_empty() {
            return Err(XraftError::BootstrapPreconditionFailed(
                "initial_voters must not be empty".into(),
            ));
        }

        self.cluster_id = Some(cluster_id);
        self.initial_voters = Some(initial_voters);
        self.bootstrapped = true;

        Ok(())
    }

    /// Gracefully shut down the node.
    ///
    /// 1. Invokes `Listener::begin_shutdown()` synchronously. In Phase 4+
    ///    this call will be dispatched through the event loop to ensure
    ///    single-threaded access; in Phase 1.7 it is called directly
    ///    since no event loop is running.
    /// 2. Drops the `propose_tx` sender (closing the mpsc channel). When
    ///    the ReceiverTask is running (Phase 4+), this causes it to exit
    ///    per architecture §4.4.
    /// 3. Awaits both `event_loop_handle` and `receiver_task_handle`
    ///    if they exist (they are `None` until tasks are spawned in
    ///    Phase 4).
    pub async fn shutdown(mut self) -> XraftResult<()> {
        // Invoke begin_shutdown on the listener. In Phase 4+ this
        // is dispatched through the event loop; in Phase 1.7 it is
        // called directly since no event loop exists.
        if let Some(ref mut listener) = self.listener {
            listener.begin_shutdown();
        }
        // Prevent further use of the listener.
        self.listener = None;

        // Drop the propose sender to close the mpsc channel.
        // In Phase 4+ this causes the ReceiverTask to exit.
        drop(self.propose_tx);

        // Await event loop handle if present (Phase 4+).
        if let Some(handle) = self.event_loop_handle.take() {
            handle.await.map_err(|_| XraftError::Shutdown)?;
        }

        // Await receiver task handle if present (Phase 4+).
        if let Some(handle) = self.receiver_task_handle.take() {
            handle.await.map_err(|_| XraftError::Shutdown)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_record::{AppRecord, AppSnapshot};
    use crate::config::RaftConfig;
    use crate::log_entry::LogEntry;
    use crate::quorum_state::QuorumState;
    use crate::rpc::{RpcEnvelope, SnapshotId};
    use crate::snapshot::{Snapshot, SnapshotWriter};
    use crate::types::{ClusterId, NodeId, Term};
    use crate::voter::VoterInfo;
    use async_trait::async_trait;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::time::Duration;

    // ── Mock implementations ──

    struct MockLogStore {
        end_offset: AtomicU64,
    }

    impl MockLogStore {
        fn new() -> Self {
            Self {
                end_offset: AtomicU64::new(0),
            }
        }

        fn with_end_offset(offset: u64) -> Self {
            Self {
                end_offset: AtomicU64::new(offset),
            }
        }
    }

    #[async_trait]
    impl LogStore for MockLogStore {
        async fn append(&self, entries: &[LogEntry]) -> XraftResult<()> {
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
    }

    impl MockQuorumStateStore {
        fn new() -> Self {
            Self {
                state: Mutex::new(None),
            }
        }

        fn with_existing() -> Self {
            Self {
                state: Mutex::new(Some(QuorumState {
                    current_term: Term(1),
                    voted_for: None,
                    leader_id: None,
                    leader_epoch: Term(0),
                })),
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
            Ok(())
        }
    }

    struct MockSnapshotIO {
        has_snapshot: bool,
    }

    impl MockSnapshotIO {
        fn new() -> Self {
            Self {
                has_snapshot: false,
            }
        }

        fn with_existing() -> Self {
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
                    metadata: crate::snapshot::SnapshotMetadata {
                        last_included_offset: 0,
                        last_included_term: Term(0),
                        voters: Vec::new(),
                        leader_epoch: Term(0),
                    },
                    app_snapshot: AppSnapshot {
                        data: Vec::new(),
                    },
                }))
            } else {
                Ok(None)
            }
        }
        async fn read_chunk(
            &self,
            _id: &SnapshotId,
            _pos: u64,
            _max: u32,
        ) -> XraftResult<(bytes::Bytes, bool)> {
            Ok((bytes::Bytes::new(), true))
        }
        async fn begin_receive(&self, _id: &SnapshotId) -> XraftResult<SnapshotWriter> {
            Ok(SnapshotWriter { data: Vec::new() })
        }
    }

    struct MockTransportSender;

    #[async_trait]
    impl TransportSender for MockTransportSender {
        async fn send(&self, _target: NodeId, _msg: RpcEnvelope) -> XraftResult<()> {
            Ok(())
        }
    }

    struct MockTransportReceiver;

    #[async_trait]
    impl TransportReceiver for MockTransportReceiver {
        async fn recv(&mut self) -> XraftResult<RpcEnvelope> {
            std::future::pending().await
        }
    }

    struct MockClock;

    #[async_trait]
    impl Clock for MockClock {
        fn now(&self) -> tokio::time::Instant {
            tokio::time::Instant::now()
        }
        async fn sleep_until(&self, deadline: tokio::time::Instant) {
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
        fn new() -> (Self, Arc<AtomicBool>) {
            let flag = Arc::new(AtomicBool::new(false));
            (
                Self {
                    shutdown_called: flag.clone(),
                },
                flag,
            )
        }
    }

    impl Listener for MockListener {
        fn handle_commit(&mut self, _batch: &[(u64, AppRecord)]) {}
        fn handle_load_snapshot(&mut self, _reader: crate::snapshot::SnapshotReader) {}
        fn handle_leader_change(&mut self, _leader_id: NodeId, _term: Term) {}
        fn begin_shutdown(&mut self) {
            self.shutdown_called.store(true, Ordering::SeqCst);
        }
    }

    fn test_config() -> RaftConfig {
        RaftConfig::default()
    }

    fn test_config_with_node_id(id: u64) -> RaftConfig {
        RaftConfig {
            node_id: NodeId(id),
            ..RaftConfig::default()
        }
    }

    fn test_voters() -> Vec<VoterInfo> {
        vec![
            VoterInfo {
                node_id: NodeId(1),
                endpoint: "127.0.0.1:9000".parse::<SocketAddr>().unwrap(),
            },
            VoterInfo {
                node_id: NodeId(2),
                endpoint: "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            },
            VoterInfo {
                node_id: NodeId(3),
                endpoint: "127.0.0.1:9002".parse::<SocketAddr>().unwrap(),
            },
        ]
    }

    fn test_cluster_id() -> ClusterId {
        ClusterId(uuid::Uuid::new_v4())
    }

    fn build_node() -> (RaftNode<MockStateMachine, MockListener>, Arc<AtomicBool>) {
        let (listener, shutdown_flag) = MockListener::new();
        let node = RaftNode::new(
            test_config(),
            Box::new(MockLogStore::new()),
            Box::new(MockQuorumStateStore::new()),
            Box::new(MockSnapshotIO::new()),
            Box::new(MockTransportSender),
            Box::new(MockTransportReceiver),
            Box::new(MockClock),
            MockStateMachine,
            listener,
        )
        .expect("construction should succeed");
        (node, shutdown_flag)
    }

    fn build_node_with_stores(
        log_store: MockLogStore,
        qs_store: MockQuorumStateStore,
        snap_io: MockSnapshotIO,
    ) -> (RaftNode<MockStateMachine, MockListener>, Arc<AtomicBool>) {
        let (listener, shutdown_flag) = MockListener::new();
        let node = RaftNode::new(
            test_config(),
            Box::new(log_store),
            Box::new(qs_store),
            Box::new(snap_io),
            Box::new(MockTransportSender),
            Box::new(MockTransportReceiver),
            Box::new(MockClock),
            MockStateMachine,
            listener,
        )
        .expect("construction should succeed");
        (node, shutdown_flag)
    }

    // ── Construction tests ──

    /// new() is safe to call outside a Tokio runtime — no tasks are spawned.
    #[test]
    fn new_succeeds_without_tokio_runtime() {
        let (node, _) = build_node();
        let state = node.read().unwrap();
        assert_eq!(state.role, Role::Unattached);
        assert!(node.cluster_id.is_none());
        assert!(node.initial_voters.is_none());
        // No task handles exist in Phase 1.7.
        assert!(node.event_loop_handle.is_none());
        assert!(node.receiver_task_handle.is_none());
    }

    #[test]
    fn new_fails_with_invalid_config() {
        let mut config = test_config();
        config.fetch_interval_ms = 999;
        config.election_timeout_min_ms = 100;
        let (listener, _) = MockListener::new();
        let result = RaftNode::new(
            config,
            Box::new(MockLogStore::new()),
            Box::new(MockQuorumStateStore::new()),
            Box::new(MockSnapshotIO::new()),
            Box::new(MockTransportSender),
            Box::new(MockTransportReceiver),
            Box::new(MockClock),
            MockStateMachine,
            listener,
        );
        assert!(result.is_err());
    }

    // ── read() tests ──

    #[test]
    fn read_returns_initial_unattached_state() {
        let (node, _) = build_node();
        let state = node.read().unwrap();
        assert_eq!(state.current_term, Term(0));
        assert_eq!(state.role, Role::Unattached);
        assert!(state.leader_id.is_none());
        assert_eq!(state.high_watermark, 0);
        assert_eq!(state.log_end_offset, 0);
        assert!(state.voter_set.is_empty());
    }

    #[test]
    fn read_returns_configured_node_id() {
        let (listener, _) = MockListener::new();
        let node = RaftNode::new(
            test_config_with_node_id(42),
            Box::new(MockLogStore::new()),
            Box::new(MockQuorumStateStore::new()),
            Box::new(MockSnapshotIO::new()),
            Box::new(MockTransportSender),
            Box::new(MockTransportReceiver),
            Box::new(MockClock),
            MockStateMachine,
            listener,
        )
        .unwrap();
        let state = node.read().unwrap();
        assert_eq!(state.node_id, NodeId(42));
    }

    // ── propose() tests ──

    #[tokio::test]
    async fn propose_returns_not_leader_when_unattached() {
        let (node, _) = build_node();
        let record = AppRecord {
            data: bytes::Bytes::from_static(b"test"),
        };
        let result = node.propose(record).await;
        assert!(
            matches!(result, Err(XraftError::NotLeader { leader_id: None })),
            "expected NotLeader with no leader, got {result:?}"
        );
    }

    // ── bootstrap() tests ──

    #[tokio::test]
    async fn bootstrap_succeeds_on_clean_state() {
        let (mut node, _) = build_node();
        let cid = test_cluster_id();
        let voters = test_voters();
        let result = node.bootstrap(cid, voters.clone()).await;
        assert!(result.is_ok(), "bootstrap should succeed: {result:?}");
    }

    #[tokio::test]
    async fn bootstrap_stores_cluster_id() {
        let (mut node, _) = build_node();
        let cid = test_cluster_id();
        node.bootstrap(cid, test_voters()).await.unwrap();
        assert_eq!(node.cluster_id, Some(cid));
    }

    #[tokio::test]
    async fn bootstrap_stores_initial_voters_in_memory() {
        let (mut node, _) = build_node();
        let voters = test_voters();
        node.bootstrap(test_cluster_id(), voters.clone())
            .await
            .unwrap();
        assert_eq!(node.initial_voters, Some(voters));
    }

    #[tokio::test]
    async fn bootstrap_does_not_persist_to_log_in_skeleton() {
        let (mut node, _) = build_node();
        node.bootstrap(test_cluster_id(), test_voters())
            .await
            .unwrap();
        assert_eq!(node.log_store.log_end_offset(), 0);
    }

    #[tokio::test]
    async fn bootstrap_does_not_persist_quorum_state_in_skeleton() {
        let (mut node, _) = build_node();
        node.bootstrap(test_cluster_id(), test_voters())
            .await
            .unwrap();
        let loaded = node.quorum_state_store.load().await.unwrap();
        assert!(loaded.is_none(), "quorum state should NOT be persisted in skeleton");
    }

    #[tokio::test]
    async fn bootstrap_does_not_update_watch_in_skeleton() {
        let (mut node, _) = build_node();
        node.bootstrap(test_cluster_id(), test_voters())
            .await
            .unwrap();
        let state = node.read().unwrap();
        assert!(state.voter_set.is_empty());
        assert_eq!(state.log_end_offset, 0);
        assert_eq!(state.role, Role::Unattached);
    }

    #[tokio::test]
    async fn bootstrap_fails_when_log_not_empty() {
        let (mut node, _) = build_node_with_stores(
            MockLogStore::with_end_offset(5),
            MockQuorumStateStore::new(),
            MockSnapshotIO::new(),
        );
        let result = node.bootstrap(test_cluster_id(), test_voters()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::BootstrapPreconditionFailed(msg) => {
                assert!(
                    msg.contains("log is not empty"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected BootstrapPreconditionFailed, got {other:?}"),
        }
        assert!(node.cluster_id.is_none());
    }

    #[tokio::test]
    async fn bootstrap_fails_when_quorum_state_exists() {
        let (mut node, _) = build_node_with_stores(
            MockLogStore::new(),
            MockQuorumStateStore::with_existing(),
            MockSnapshotIO::new(),
        );
        let result = node.bootstrap(test_cluster_id(), test_voters()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::BootstrapPreconditionFailed(msg) => {
                assert!(
                    msg.contains("quorum-state already exists"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected BootstrapPreconditionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bootstrap_fails_when_snapshot_exists() {
        let (mut node, _) = build_node_with_stores(
            MockLogStore::new(),
            MockQuorumStateStore::new(),
            MockSnapshotIO::with_existing(),
        );
        let result = node.bootstrap(test_cluster_id(), test_voters()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::BootstrapPreconditionFailed(msg) => {
                assert!(
                    msg.contains("snapshot already exists"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected BootstrapPreconditionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bootstrap_fails_when_already_bootstrapped() {
        let (mut node, _) = build_node();
        node.bootstrap(test_cluster_id(), test_voters())
            .await
            .unwrap();
        let result = node.bootstrap(test_cluster_id(), test_voters()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::BootstrapPreconditionFailed(msg) => {
                assert!(
                    msg.contains("already been bootstrapped"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected BootstrapPreconditionFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bootstrap_fails_when_initial_voters_empty() {
        let (mut node, _) = build_node();
        let result = node.bootstrap(test_cluster_id(), vec![]).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::BootstrapPreconditionFailed(msg) => {
                assert!(
                    msg.contains("initial_voters must not be empty"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected BootstrapPreconditionFailed, got {other:?}"),
        }
    }

    // ── shutdown() tests ──

    #[tokio::test]
    async fn shutdown_invokes_begin_shutdown() {
        let (node, shutdown_flag) = build_node();
        assert!(!shutdown_flag.load(Ordering::SeqCst));
        node.shutdown().await.unwrap();
        assert!(
            shutdown_flag.load(Ordering::SeqCst),
            "begin_shutdown should have been called"
        );
    }

    #[tokio::test]
    async fn shutdown_succeeds_with_no_tasks() {
        let (node, _) = build_node();
        node.shutdown().await.unwrap();
    }

    // ── Integration-style tests ──

    #[tokio::test]
    async fn full_lifecycle_construct_bootstrap_read_shutdown() {
        let (mut node, shutdown_flag) = build_node();

        let state = node.read().unwrap();
        assert_eq!(state.role, Role::Unattached);
        assert!(state.voter_set.is_empty());
        assert_eq!(state.node_id, NodeId(1)); // default config node_id

        let cid = test_cluster_id();
        node.bootstrap(cid, test_voters()).await.unwrap();

        // Skeleton stores in memory only, watch unchanged.
        let state = node.read().unwrap();
        assert!(state.voter_set.is_empty());
        assert_eq!(state.log_end_offset, 0);

        assert_eq!(node.cluster_id, Some(cid));

        let result = node
            .propose(AppRecord {
                data: bytes::Bytes::from_static(b"cmd"),
            })
            .await;
        assert!(matches!(result, Err(XraftError::NotLeader { .. })));

        node.shutdown().await.unwrap();
        assert!(shutdown_flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn read_unaffected_by_skeleton_bootstrap() {
        let (mut node, _) = build_node();

        let before = node.read().unwrap();
        assert!(before.voter_set.is_empty());
        assert_eq!(before.log_end_offset, 0);

        node.bootstrap(test_cluster_id(), test_voters())
            .await
            .unwrap();

        let after = node.read().unwrap();
        assert!(after.voter_set.is_empty());
        assert_eq!(after.log_end_offset, 0);
        assert_eq!(after.role, Role::Unattached);
    }
}
