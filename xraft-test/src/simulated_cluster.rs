use std::collections::HashMap;
use xraft_core::*;
use xraft_core::log_entry::LogEntry;
use xraft_core::rpc::{RpcEnvelope, RpcPayload};
use xraft_core::traits::{LogStore, QuorumStateStore};
use xraft_core::io_action::{IoAction, IoActionBatch};
use xraft_core::io_stage::IoStage;
use xraft_core::event_loop::{EventLoopDriver, InboundMessage};
use crate::simulated_network::{MessageBus, SimulatedTransportSender};
use crate::simulated_clock::SimulatedClock;
use crate::test_state_machine::TestStateMachine;
use crate::test_listener::TestListener;

/// Synchronous executor for trait-object async methods.
/// Safe because `MemoryLogStore` and `MemoryQuorumStateStore` use
/// `std::sync::RwLock` internally, so the futures always resolve in
/// one poll without yielding. The `SimulatedTransportSender` uses
/// `std::sync::Mutex` and likewise never yields.
///
/// IMPORTANT: Only use with non-yielding in-memory implementations.
/// Any implementation that genuinely awaits will panic here.
fn poll_now<T>(future: impl std::future::Future<Output = T>) -> T {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    unsafe fn clone_fn(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }
    unsafe fn nop_fn(_: *const ()) {}
    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_fn, nop_fn, nop_fn, nop_fn);

    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("trait object implementation should never yield in SimulatedCluster"),
    }
}

/// A simulated multi-node Raft cluster for deterministic integration testing.
///
/// Exercises the full architecture pipeline:
/// 1. Inbound messages are processed by the `EventLoopDriver` which mutates
///    `NodeState`, invokes callbacks (StateMachine::apply, Listener::handle_commit,
///    DeferredCompletionQueue::complete), and produces `IoActionBatch`.
/// 2. `IoActionBatch` is executed by the `IoStage` which dispatches through
///    `LogStore`, `QuorumStateStore`, and `TransportSender` trait objects.
/// 3. `TransportSender` routes RPC envelopes through a shared `MessageBus`.
/// 4. The cluster drains the bus and delivers to target EventLoopDrivers.
///
/// This validates the full IoAction/IoStage/EventLoop pipeline from
/// architecture §4.1 while maintaining deterministic message ordering.
pub struct SimulatedCluster {
    /// Per-node EventLoopDrivers — own the RaftNode, StateMachine, Listener, and DCQ.
    drivers: HashMap<NodeId, EventLoopDriver<TestStateMachine, TestListener>>,

    /// Per-node IoStages — own the LogStore, QuorumStateStore, and TransportSender.
    io_stages: HashMap<NodeId, IoStage>,

    /// Shared message bus for deterministic message delivery.
    message_bus: MessageBus,

    pub partitioned: Vec<(NodeId, NodeId)>,
    pub stopped_nodes: Vec<NodeId>,
    clock: SimulatedClock,
    config: RaftConfig,
    pub cluster_id: String,
}

impl SimulatedCluster {
    /// Create a new simulated cluster with the given number of nodes.
    ///
    /// Each node gets:
    /// - An `EventLoopDriver` (owns RaftNode, TestStateMachine, TestListener, DeferredCompletionQueue)
    /// - An `IoStage` (owns Box<dyn LogStore>, Box<dyn QuorumStateStore>, Box<dyn TransportSender>)
    pub fn new(node_count: u64) -> Self {
        let config = RaftConfig::default();
        let voter_set: Vec<VoterInfo> = (1..=node_count)
            .map(|i| VoterInfo {
                node_id: NodeId(i),
                endpoint: format!("127.0.0.1:{}", 9000 + i),
            })
            .collect();

        let mut drivers = HashMap::new();
        let mut io_stages = HashMap::new();
        let message_bus = MessageBus::new();
        let cluster_id = "test-cluster".to_string();

        for info in &voter_set {
            let mut node = RaftNode::new(info.node_id, voter_set.clone(), config.clone());
            node.bootstrap();

            let sm = TestStateMachine::new();
            let listener = TestListener::new();
            let driver = EventLoopDriver::new(
                node,
                sm,
                listener,
                cluster_id.clone(),
                config.clone(),
            );
            drivers.insert(info.node_id, driver);

            let log_store: Box<dyn LogStore> =
                Box::new(xraft_storage::MemoryLogStore::new());
            let quorum_store: Box<dyn QuorumStateStore> =
                Box::new(xraft_storage::MemoryQuorumStateStore::new());
            let transport: Box<dyn xraft_core::traits::TransportSender> =
                Box::new(SimulatedTransportSender::new(
                    info.node_id,
                    message_bus.clone(),
                ));
            let io_stage = IoStage::new(log_store, quorum_store, transport);
            io_stages.insert(info.node_id, io_stage);
        }

        Self {
            drivers,
            io_stages,
            message_bus,
            partitioned: Vec::new(),
            stopped_nodes: Vec::new(),
            clock: SimulatedClock::new(config.election_timeout_min_ms),
            config,
            cluster_id,
        }
    }

    // -----------------------------------------------------------------------
    // Node access (delegates to EventLoopDriver fields)
    // -----------------------------------------------------------------------

    /// Get a reference to a node's RaftNode.
    pub fn node(&self, id: NodeId) -> &RaftNode {
        &self.drivers.get(&id).expect("Node not found").node
    }

    /// Get a mutable reference to a node's RaftNode.
    pub fn node_mut(&mut self, id: NodeId) -> &mut RaftNode {
        &mut self.drivers.get_mut(&id).expect("Node not found").node
    }

    /// Public access to state_machines through drivers.
    pub fn state_machine(&self, id: NodeId) -> &TestStateMachine {
        &self.drivers.get(&id).unwrap().state_machine
    }

    /// Public access to listener through drivers.
    pub fn listener(&self, id: NodeId) -> &TestListener {
        &self.drivers.get(&id).unwrap().listener
    }

    /// Public access to the DeferredCompletionQueue for a node.
    pub fn completion_queue(
        &self,
        id: NodeId,
    ) -> &xraft_core::deferred_completion::DeferredCompletionQueue {
        &self.drivers.get(&id).unwrap().completion_queue
    }

    /// Check if two nodes can communicate (not partitioned, both alive).
    pub fn can_communicate(&self, a: NodeId, b: NodeId) -> bool {
        if self.stopped_nodes.contains(&a) || self.stopped_nodes.contains(&b) {
            return false;
        }
        !self.partitioned
            .iter()
            .any(|(x, y)| (*x == a && *y == b) || (*x == b && *y == a))
    }

    /// Advance the simulated clock.
    pub fn advance_time(&mut self, ms: u64) {
        self.clock.advance(ms);
    }

    /// Get current simulated time.
    pub fn now_ms(&self) -> u64 {
        self.clock.now()
    }

    // -----------------------------------------------------------------------
    // IoStage execution
    // -----------------------------------------------------------------------

    /// Execute an IoActionBatch through a node's IoStage.
    /// Filters SendRpc actions through partition/stopped checks.
    fn execute_batch(&self, from: NodeId, batch: &IoActionBatch) {
        // We need to filter SendRpc actions through partition checks.
        // For non-SendRpc actions, execute directly through IoStage.
        let stage = self.io_stages.get(&from).unwrap();

        for action in &batch.actions {
            match action {
                IoAction::SendRpc(target, envelope) => {
                    // Route through IoStage's transport (MessageBus)
                    // Partition filtering happens at delivery time
                    poll_now(stage.execute(&IoActionBatch {
                        actions: vec![IoAction::SendRpc(*target, envelope.clone())],
                    }))
                    .unwrap();
                }
                IoAction::AppendLog(entries) => {
                    poll_now(stage.execute(&IoActionBatch {
                        actions: vec![IoAction::AppendLog(entries.clone())],
                    }))
                    .unwrap();
                }
                IoAction::TruncateSuffix(offset) => {
                    poll_now(stage.execute(&IoActionBatch {
                        actions: vec![IoAction::TruncateSuffix(*offset)],
                    }))
                    .unwrap();
                }
                IoAction::PersistQuorumState(qs) => {
                    poll_now(stage.execute(&IoActionBatch {
                        actions: vec![IoAction::PersistQuorumState(qs.clone())],
                    }))
                    .unwrap();
                }
                IoAction::TruncatePrefix(offset) => {
                    poll_now(stage.execute(&IoActionBatch {
                        actions: vec![IoAction::TruncatePrefix(*offset)],
                    }))
                    .unwrap();
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Message bus delivery
    // -----------------------------------------------------------------------

    /// Drain the message bus and dispatch each message to the target node's
    /// EventLoopDriver. Responses produce IoActionBatches which are executed
    /// through the target's IoStage.
    fn deliver_pending(&mut self) {
        let messages = self.message_bus.drain();

        for (target, envelope) in messages {
            if envelope.cluster_id != self.cluster_id {
                continue;
            }
            if !self.can_communicate(envelope.source, target) {
                continue;
            }

            let source = envelope.source;
            let now = self.clock.now();

            let msg = match envelope.payload {
                RpcPayload::VoteRequest(req) => InboundMessage::VoteRequest {
                    from: source,
                    request: req,
                },
                RpcPayload::VoteResponse(resp) => InboundMessage::VoteResponse {
                    from: source,
                    response: resp,
                },
                RpcPayload::FetchRequest(req) => InboundMessage::FetchRequest {
                    from: source,
                    request: req,
                },
                RpcPayload::FetchResponse(resp) => InboundMessage::FetchResponse {
                    from: source,
                    response: resp,
                },
            };

            // Process through EventLoopDriver → IoActionBatch
            let driver = self.drivers.get_mut(&target).unwrap();
            let (batch, _completion_rx) = driver.process(msg, now);

            // Execute through IoStage
            self.execute_batch(target, &batch);
        }
    }

    // -----------------------------------------------------------------------
    // High-level cluster operations
    // -----------------------------------------------------------------------

    /// Elect a specific node as leader using the EventLoopDriver pipeline.
    /// VoteRequest/VoteResponse messages traverse:
    ///   EventLoopDriver::start_election → IoActionBatch → IoStage::execute
    ///   → TransportSender → MessageBus → deliver_pending → EventLoopDriver::process
    pub fn elect_leader(&mut self, leader_id: NodeId) {
        let now = self.clock.now();

        // Start election through EventLoopDriver
        let driver = self.drivers.get_mut(&leader_id).unwrap();
        let batch = driver.start_election(now);

        // Execute IoActions (sends VoteRequests + persists quorum state)
        self.execute_batch(leader_id, &batch);

        // Phase 1: Deliver VoteRequests → voters process, send VoteResponses
        self.deliver_pending();

        // Phase 2: Deliver VoteResponses → candidate processes, becomes leader
        self.deliver_pending();

        assert!(
            self.node(leader_id).is_leader(),
            "Failed to elect {:?} as leader",
            leader_id
        );
    }

    /// Run one round of Fetch from all followers to the leader.
    /// All messages traverse: EventLoopDriver → IoActionBatch → IoStage → TransportSender.
    pub fn run_fetch_round(&mut self) -> Option<NodeId> {
        let leader_id = self.find_leader()?;

        let follower_ids: Vec<NodeId> = self
            .drivers
            .keys()
            .filter(|id| **id != leader_id && !self.stopped_nodes.contains(id))
            .copied()
            .collect();

        // Phase 1: Followers build FetchRequests and send via IoStage
        for fid in &follower_ids {
            let driver = self.drivers.get(fid).unwrap();
            let fetch_req = driver.build_fetch_request();
            let envelope = RpcEnvelope {
                cluster_id: self.cluster_id.clone(),
                source: *fid,
                leader_epoch: driver.node.term().0,
                payload: RpcPayload::FetchRequest(fetch_req),
            };
            let stage = self.io_stages.get(fid).unwrap();
            poll_now(stage.execute(&IoActionBatch {
                actions: vec![IoAction::SendRpc(leader_id, envelope)],
            }))
            .unwrap();
        }

        // Phase 2: Deliver FetchRequests → leader's EventLoopDriver processes,
        //          produces FetchResponse IoActions → IoStage sends them
        self.deliver_pending();

        // Phase 3: Deliver FetchResponses → followers' EventLoopDrivers process,
        //          invoke SM::apply + Listener + DCQ if HW advances
        self.deliver_pending();

        Some(leader_id)
    }

    /// Run multiple fetch rounds until convergence.
    pub fn replicate_fully(&mut self, max_rounds: usize) {
        for _ in 0..max_rounds {
            self.run_fetch_round();
            if self.all_converged() {
                return;
            }
        }
    }

    /// Check if all active nodes have converged.
    pub fn all_converged(&self) -> bool {
        let active: Vec<&RaftNode> = self
            .drivers
            .values()
            .filter(|d| !self.stopped_nodes.contains(&d.node.node_id()))
            .map(|d| &d.node)
            .collect();

        if active.len() < 2 {
            return true;
        }

        let target_leo = active[0].log_end_offset();
        let target_hw = active[0].high_watermark();

        active
            .iter()
            .all(|n| n.log_end_offset() == target_leo && n.high_watermark() == target_hw)
    }

    /// Find the current leader among active nodes.
    pub fn find_leader(&self) -> Option<NodeId> {
        self.drivers
            .values()
            .filter(|d| !self.stopped_nodes.contains(&d.node.node_id()))
            .find(|d| d.node.is_leader())
            .map(|d| d.node.node_id())
    }

    /// Wait for a leader to emerge.
    pub fn wait_for_leader(&mut self, preferred: Option<NodeId>) -> NodeId {
        if let Some(lid) = self.find_leader() {
            return lid;
        }
        let candidate = preferred.unwrap_or_else(|| {
            *self
                .drivers
                .keys()
                .find(|id| !self.stopped_nodes.contains(id))
                .expect("no active nodes")
        });
        self.advance_time(self.config.election_timeout_max_ms + 1);
        self.elect_leader(candidate);
        candidate
    }

    /// Propose a command through the leader's EventLoopDriver.
    /// Returns the offset and a oneshot receiver for commit notification.
    pub fn propose(
        &mut self,
        record: &AppRecord,
    ) -> Option<u64> {
        let leader_id = self.find_leader()?;
        self.propose_to(leader_id, record)
    }

    /// Propose to a specific node through its EventLoopDriver.
    pub fn propose_to(
        &mut self,
        node_id: NodeId,
        record: &AppRecord,
    ) -> Option<u64> {
        let now = self.clock.now();
        let driver = self.drivers.get_mut(&node_id)?;
        let msg = InboundMessage::Propose(record.clone());
        let (batch, _completion_rx) = driver.process(msg, now);
        let leo = driver.node.log_end_offset();

        // Execute IoActions (AppendLog, PersistQuorumState)
        self.execute_batch(node_id, &batch);

        if leo > 0 {
            Some(leo - 1)
        } else {
            None
        }
    }

    /// Propose and return the completion receiver (for DCQ testing).
    pub fn propose_with_completion(
        &mut self,
        record: &AppRecord,
    ) -> Option<(u64, tokio::sync::oneshot::Receiver<u64>)> {
        let leader_id = self.find_leader()?;
        let now = self.clock.now();
        let driver = self.drivers.get_mut(&leader_id)?;
        let msg = InboundMessage::Propose(record.clone());
        let (batch, completion_rx) = driver.process(msg, now);
        let leo = driver.node.log_end_offset();

        self.execute_batch(leader_id, &batch);

        if leo > 0 {
            completion_rx.map(|rx| (leo - 1, rx))
        } else {
            None
        }
    }

    /// Stop a node (simulate crash). Persists state via IoStage before stopping.
    pub fn stop_node(&mut self, id: NodeId) {
        self.stopped_nodes.push(id);
    }

    /// Restart a stopped node — recovers from persistent trait-object stores.
    pub fn restart_node(&mut self, id: NodeId) {
        self.stopped_nodes.retain(|nid| *nid != id);

        // Recover log from LogStore trait object via IoStage
        let stage = self.io_stages.get(&id).unwrap();
        let store = stage.log_store();
        let store_leo = store.log_end_offset();
        let recovered_log = poll_now(store.read(0, store_leo)).unwrap();

        // Recover quorum state from QuorumStateStore via IoStage
        let qs_store = stage.quorum_store();
        let recovered_qs = poll_now(qs_store.load()).unwrap();

        if let Some(driver) = self.drivers.get_mut(&id) {
            driver.node.state.log = recovered_log;

            if let Some(qs) = recovered_qs {
                driver.node.state.current_term = qs.current_term;
                driver.node.state.voted_for = None;
            }

            driver.node.state.high_watermark = 0;
            driver.node.state.last_applied = 0;
            driver.node.state.role = Role::Follower;
            driver.node.state.leader_id = None;
            driver.node.state.votes_received.clear();
            driver.node.state.follower_progress.clear();

            // Rebuild leader epoch checkpoint by scanning recovered log
            driver.node.state.leader_epoch_checkpoint.clear();
            let log_entries: Vec<LogEntry> = driver.node.state.log.clone();
            for entry in &log_entries {
                let prev_epoch = driver
                    .node
                    .state
                    .leader_epoch_checkpoint
                    .last()
                    .map(|e| e.epoch);
                if prev_epoch != Some(entry.term.0) {
                    driver
                        .node
                        .state
                        .leader_epoch_checkpoint
                        .push(EpochEntry {
                            epoch: entry.term.0,
                            start_offset: entry.offset,
                        });
                }
            }

            // Reset state machine and listener — volatile state lost on crash
            driver.state_machine.reset();
            driver.listener.reset();
        }
    }

    /// Add a partition between two nodes.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitioned.push((a, b));
    }

    /// Remove all partitions.
    pub fn heal_partitions(&mut self) {
        self.partitioned.clear();
    }

    /// Get log entries for a node.
    pub fn node_log(&self, id: NodeId) -> Vec<LogEntry> {
        self.node(id).log().to_vec()
    }

    /// Get the high watermark for a node.
    pub fn node_hw(&self, id: NodeId) -> u64 {
        self.node(id).high_watermark()
    }

    /// Get the log_end_offset for a node.
    pub fn node_leo(&self, id: NodeId) -> u64 {
        self.node(id).log_end_offset()
    }

    /// Get the number of voters.
    pub fn voter_count(&self) -> usize {
        self.drivers
            .values()
            .next()
            .map(|d| d.node.state.voter_set.len())
            .unwrap_or(0)
    }

    /// Verify that a node's LogStore (via IoStage) matches its in-memory log.
    pub fn verify_storage_consistency(&self, id: NodeId) -> bool {
        let driver = self.drivers.get(&id).unwrap();
        let stage = self.io_stages.get(&id).unwrap();
        let store = stage.log_store();
        let store_leo = store.log_end_offset();
        let node_leo = driver.node.log_end_offset();

        if store_leo != node_leo {
            return false;
        }

        let stored = poll_now(store.read(0, store_leo)).unwrap();
        let in_memory = &driver.node.state.log;

        if stored.len() != in_memory.len() {
            return false;
        }

        stored
            .iter()
            .zip(in_memory.iter())
            .all(|(s, m)| s.offset == m.offset && s.term == m.term && s.data == m.data)
    }

    /// Get a reference to a node's LogStore trait object (via IoStage).
    pub fn log_store(&self, id: NodeId) -> &dyn LogStore {
        self.io_stages.get(&id).unwrap().log_store()
    }

    /// Get a reference to a node's QuorumStateStore trait object (via IoStage).
    pub fn quorum_store(&self, id: NodeId) -> &dyn QuorumStateStore {
        self.io_stages.get(&id).unwrap().quorum_store()
    }

    /// Get all node IDs in the cluster.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.drivers.keys().copied().collect()
    }
}
