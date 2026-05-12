use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use xraft_core::config::RaftConfig;
use xraft_core::consensus_state::Role;
use xraft_core::election::ElectionManager;
use xraft_core::event_loop::{EventLoop, EventLoopMessage};
use xraft_core::io_action::IoStage;
use xraft_core::log_entry::EntryType;
use xraft_core::node_state::NodeState;
use xraft_core::rpc::{
    FetchRequest, FetchResponse, FetchSnapshotRequest, RpcEnvelope, RpcPayload, SnapshotId,
};
use xraft_core::types::{ClusterId, NodeId, Term};
use xraft_core::voter::VoterInfo;

use crate::simulated_clock::SimulatedClock;
use crate::test_harness::{
    InMemoryLogStore, InMemoryQuorumStateStore, NullSnapshotIO, NullStateMachine, NullTransport,
    RecordingListener,
};

fn test_config() -> RaftConfig {
    RaftConfig {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        fetch_interval_ms: 50,
        max_batch_size: 256,
        max_fetch_bytes: 1024 * 1024,
        snapshot_interval: 10_000,
        data_dir: std::path::PathBuf::from("test_data"),
    }
}

fn test_cluster_id() -> ClusterId {
    ClusterId(uuid::Uuid::nil())
}

fn three_node_voters() -> Vec<VoterInfo> {
    vec![
        VoterInfo {
            node_id: NodeId(1),
            endpoint: "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
        },
        VoterInfo {
            node_id: NodeId(2),
            endpoint: "127.0.0.1:9002".parse::<SocketAddr>().unwrap(),
        },
        VoterInfo {
            node_id: NodeId(3),
            endpoint: "127.0.0.1:9003".parse::<SocketAddr>().unwrap(),
        },
    ]
}

/// Build an EventLoop for N1 in a 3-node cluster, starting as Unattached.
/// The supplied `log_store` Arc is shared with the IoStage so tests can
/// inspect persisted entries after event-loop operations.
fn build_event_loop(
    clock: &SimulatedClock,
    log_store: &Arc<InMemoryLogStore>,
) -> (
    EventLoop<NullStateMachine, RecordingListener>,
    mpsc::Sender<EventLoopMessage>,
    watch::Receiver<xraft_core::ConsensusState>,
    RecordingListener,
) {
    let config = test_config();
    let now = clock.current();
    let voters = three_node_voters();
    let state = NodeState::new(NodeId(1), test_cluster_id(), voters, now);

    // Use the SHARED log_store so the test can read back persisted entries.
    let io_stage = IoStage::new(
        Box::new(ArcLogStore(Arc::clone(log_store))),
        Box::new(NullTransport),
        Box::new(InMemoryQuorumStateStore::new()),
        Box::new(NullSnapshotIO),
    );

    let listener = RecordingListener::new();
    let (msg_tx, msg_rx) = mpsc::channel(64);
    let initial_consensus = state.project();
    let (state_tx, state_rx) = watch::channel(initial_consensus);

    let event_loop = EventLoop::new(
        state,
        config,
        Box::new(clock.clone()),
        io_stage,
        NullStateMachine,
        listener.clone(),
        msg_rx,
        state_tx,
    );

    (event_loop, msg_tx, state_rx, listener)
}

/// Newtype wrapper that delegates LogStore to an Arc<InMemoryLogStore>.
struct ArcLogStore(Arc<InMemoryLogStore>);

#[async_trait::async_trait]
impl xraft_core::traits::LogStore for ArcLogStore {
    async fn append(&self, entries: &[xraft_core::LogEntry]) -> Result<(), std::io::Error> {
        self.0.append(entries).await
    }
    async fn read(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<xraft_core::LogEntry>, std::io::Error> {
        self.0.read(start, end).await
    }
    async fn truncate_suffix(&self, from: u64) -> Result<(), std::io::Error> {
        self.0.truncate_suffix(from).await
    }
    async fn truncate_prefix(&self, up_to: u64) -> Result<(), std::io::Error> {
        self.0.truncate_prefix(up_to).await
    }
    fn log_start_offset(&self) -> u64 {
        self.0.log_start_offset()
    }
    fn log_end_offset(&self) -> u64 {
        self.0.log_end_offset()
    }
    async fn entry_at(&self, offset: u64) -> Result<Option<xraft_core::LogEntry>, std::io::Error> {
        self.0.entry_at(offset).await
    }
}

/// Helper: make N1 a leader for term 1 by driving the election through
/// the event loop's `handle_rpc` path (VoteResponse from N2).
async fn make_leader(
    event_loop: &mut EventLoop<NullStateMachine, RecordingListener>,
    _msg_tx: &mpsc::Sender<EventLoopMessage>,
    clock: &SimulatedClock,
) {
    let now = clock.current();
    let election_timeout = Duration::from_millis(150);

    // 1. Transition to Follower so the election timer is armed
    event_loop
        .state
        .become_follower(Term(0), None, now + election_timeout);

    // 2. Advance past election timeout
    clock.advance(election_timeout + Duration::from_millis(10));

    // 3. Trigger election timeout via the event loop
    event_loop.handle_election_timeout().await.unwrap();
    assert_eq!(event_loop.state.role, Role::Candidate);
    let candidate_term = event_loop.state.current_term;

    // 4. Send a VoteResponse grant from N2 via handle_rpc
    let vote_resp_envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(2),
        payload: RpcPayload::VoteResponse(xraft_core::rpc::VoteResponse {
            term: candidate_term,
            vote_granted: true,
            is_pre_vote: false,
        }),
    };
    event_loop.handle_rpc(vote_resp_envelope).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Leader);
    assert_eq!(event_loop.state.current_term, candidate_term);
}

/// Helper to make leader at a specific term (for term-5 test).
async fn make_leader_at_term(
    event_loop: &mut EventLoop<NullStateMachine, RecordingListener>,
    _msg_tx: &mpsc::Sender<EventLoopMessage>,
    clock: &SimulatedClock,
    target_term: Term,
) {
    let election_timeout = Duration::from_millis(150);

    // Set term to target - 1, then become_candidate will increment to target
    event_loop.state.current_term = Term(target_term.0.saturating_sub(1));
    let now = clock.current();
    event_loop
        .state
        .become_follower(event_loop.state.current_term, None, now + election_timeout);

    clock.advance(election_timeout + Duration::from_millis(10));

    // Trigger election timeout
    event_loop.handle_election_timeout().await.unwrap();
    assert_eq!(event_loop.state.role, Role::Candidate);
    assert_eq!(event_loop.state.current_term, target_term);

    // Send VoteResponse grant from N2
    let vote_resp = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(2),
        payload: RpcPayload::VoteResponse(xraft_core::rpc::VoteResponse {
            term: target_term,
            vote_granted: true,
            is_pre_vote: false,
        }),
    };
    event_loop.handle_rpc(vote_resp).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Leader);
    assert_eq!(event_loop.state.current_term, target_term);
}

// ═══════════════════════════════════════════════════════════════════
// Test Scenario 1: Check Quorum Pass
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn check_quorum_pass_leader_remains() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _state_rx, _listener) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Record recent fetches from N2 and N3 via handle_rpc (envelope source)
    let fetch_env_n2 = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: event_loop.state.current_term,
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(2),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    let fetch_env_n3 = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: event_loop.state.current_term,
        source: NodeId(3),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(3),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    event_loop.handle_rpc(fetch_env_n2).await.unwrap();
    event_loop.handle_rpc(fetch_env_n3).await.unwrap();

    // Advance to check_quorum_deadline, then run check
    clock.advance(Duration::from_millis(150));
    event_loop.state.check_quorum_deadline = clock.current();
    event_loop.handle_check_quorum().await.unwrap();

    assert_eq!(event_loop.state.role, Role::Leader);
    assert_eq!(event_loop.state.leader_id, Some(NodeId(1)));
}

#[tokio::test]
async fn check_quorum_pass_with_partial_majority() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Only N2 fetches recently (leader + N2 = 2 = majority of 3)
    let fetch_env = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: event_loop.state.current_term,
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(2),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    event_loop.handle_rpc(fetch_env).await.unwrap();

    // Run check quorum
    let now = clock.current();
    let election_timeout = Duration::from_millis(150);
    assert!(
        event_loop.state.check_quorum(now, election_timeout),
        "check quorum should pass: leader(1) + N2(1) = 2 >= majority(2)"
    );
    assert_eq!(event_loop.state.role, Role::Leader);
}

// ═══════════════════════════════════════════════════════════════════
// Test Scenario 2: Check Quorum Fail — Leader Steps Down
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn check_quorum_fail_leader_steps_down() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Do NOT record any fetches from followers.
    // Because last_fetch_timestamp starts as None, check_quorum should fail
    // even without advancing time at all — no follower has ever fetched.
    let now = clock.current();
    let election_timeout = Duration::from_millis(150);
    assert!(
        !event_loop.state.check_quorum(now, election_timeout),
        "check quorum should fail when no follower has ever sent a Fetch"
    );

    // Now run handle_check_quorum via the event loop, which persists state
    // Advance past the deadline so the check fires
    clock.advance(election_timeout + Duration::from_millis(10));
    event_loop.state.check_quorum_deadline = clock.current();
    event_loop.handle_check_quorum().await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.leader_id, None);
    // Safety: voted_for must be preserved as Some(self) after check-quorum
    // step-down in the same term, so the node cannot grant a same-term vote.
    assert_eq!(event_loop.state.voted_for, Some(NodeId(1)),
        "voted_for must be preserved on check-quorum step-down to prevent same-term vote grant");
}

#[tokio::test]
async fn check_quorum_fail_stale_fetches() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Record fetches via handle_rpc
    let fetch_n2 = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: event_loop.state.current_term,
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(2),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    let fetch_n3 = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: event_loop.state.current_term,
        source: NodeId(3),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(3),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    event_loop.handle_rpc(fetch_n2).await.unwrap();
    event_loop.handle_rpc(fetch_n3).await.unwrap();

    // Advance past election timeout — fetches become stale
    let election_timeout = Duration::from_millis(150);
    clock.advance(election_timeout + Duration::from_millis(10));
    let later = clock.current();

    assert!(
        !event_loop.state.check_quorum(later, election_timeout),
        "check quorum should fail: fetches are older than election_timeout"
    );
}

#[tokio::test]
async fn check_quorum_fail_no_fetch_ever_sent() {
    // This specifically tests that become_leader initializing timestamps to None
    // means check_quorum fails immediately when no Fetch has ever arrived.
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Immediately check — no follower has fetched
    let now = clock.current();
    let election_timeout = Duration::from_millis(150);
    assert!(
        !event_loop.state.check_quorum(now, election_timeout),
        "check quorum must fail when no follower has ever sent a Fetch"
    );
}

// ═══════════════════════════════════════════════════════════════════
// Test Scenario 3: LeaderChangeMessage
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn leader_change_message_appended_on_election() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, listener) = build_event_loop(&clock, &log_store);

    // The log should be empty before becoming leader
    assert_eq!(event_loop.state.log_end_offset, 0);
    assert!(log_store.entries().is_empty());

    // Win election for term 5 via event loop flow
    make_leader_at_term(&mut event_loop, &msg_tx, &clock, Term(5)).await;

    assert_eq!(event_loop.state.role, Role::Leader);
    assert_eq!(event_loop.state.current_term, Term(5));

    // log_end_offset should have advanced
    assert_eq!(event_loop.state.log_end_offset, 1);

    // Verify the LogStore actually has the persisted entry
    let entries = log_store.entries();
    assert_eq!(entries.len(), 1, "log store should have exactly 1 entry");
    assert_eq!(entries[0].offset, 0, "first entry should be at offset 0");
    assert_eq!(entries[0].term, Term(5), "entry should be in term 5");
    assert_eq!(
        entries[0].entry_type,
        EntryType::LeaderChangeMessage,
        "entry type should be LeaderChangeMessage"
    );

    // Verify listener was notified
    let changes = listener.leader_changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].0, NodeId(1));
    assert_eq!(changes[0].1, Term(5));
}

#[tokio::test]
async fn leader_change_message_appended_via_vote_response() {
    // Test that LeaderChangeMessage is appended through the event loop's
    // VoteResponse → record_vote → on_become_leader path (not direct manipulation).
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, listener) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Verify persistence in the shared log store
    let entries = log_store.entries();
    assert!(!entries.is_empty(), "LeaderChangeMessage should be persisted");
    let lcm = entries
        .iter()
        .find(|e| e.entry_type == EntryType::LeaderChangeMessage)
        .expect("should have a LeaderChangeMessage entry");
    assert_eq!(lcm.term, event_loop.state.current_term);

    // Listener should have been notified
    let changes = listener.leader_changes();
    assert!(!changes.is_empty(), "listener should see leader change");
}

// ═══════════════════════════════════════════════════════════════════
// Test: Leader Step-Down on Higher Term
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn leader_steps_down_on_higher_term_vote_request() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    assert_eq!(event_loop.state.role, Role::Leader);
    let leader_term = event_loop.state.current_term;

    // Receive a VoteRequest with higher term via handle_rpc
    let envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(3),
        payload: RpcPayload::VoteRequest(xraft_core::rpc::VoteRequest {
            term: Term(leader_term.0 + 4),
            candidate_id: NodeId(3),
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: false,
        }),
    };
    event_loop.handle_rpc(envelope).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.current_term, Term(leader_term.0 + 4));
    assert_eq!(event_loop.state.leader_id, None);
}

#[tokio::test]
async fn leader_steps_down_on_higher_term_message() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    assert_eq!(event_loop.state.role, Role::Leader);

    let stepped_down = ElectionManager::maybe_step_down_on_higher_term(
        &mut event_loop.state,
        Term(5),
        NodeId(3),
        &SimulatedClock::new(Duration::from_millis(150)),
        &test_config(),
    );

    assert!(stepped_down);
    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.current_term, Term(5));
    assert_eq!(event_loop.state.leader_id, None);
}

#[tokio::test]
async fn leader_does_not_step_down_on_same_term() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let current = event_loop.state.current_term;

    let stepped_down = ElectionManager::maybe_step_down_on_higher_term(
        &mut event_loop.state,
        current,
        NodeId(3),
        &SimulatedClock::new(Duration::from_millis(150)),
        &test_config(),
    );

    assert!(!stepped_down);
    assert_eq!(event_loop.state.role, Role::Leader);
}

#[tokio::test]
async fn leader_does_not_step_down_on_lower_term() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    let stepped_down = ElectionManager::maybe_step_down_on_higher_term(
        &mut event_loop.state,
        Term(0),
        NodeId(3),
        &SimulatedClock::new(Duration::from_millis(150)),
        &test_config(),
    );

    assert!(!stepped_down);
    assert_eq!(event_loop.state.role, Role::Leader);
}

// ═══════════════════════════════════════════════════════════════════
// Test: Higher-term step-down on every RPC path
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn leader_steps_down_on_fetch_request_with_higher_epoch() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let leader_term = event_loop.state.current_term;

    let envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(leader_term.0 + 2),
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(2),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    event_loop.handle_rpc(envelope).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.current_term, Term(leader_term.0 + 2));
}

#[tokio::test]
async fn leader_steps_down_on_fetch_response_with_higher_epoch() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let leader_term = event_loop.state.current_term;

    let envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(leader_term.0 + 3),
        source: NodeId(2),
        payload: RpcPayload::FetchResponse(FetchResponse {
            leader_id: NodeId(2),
            leader_epoch: Term(leader_term.0 + 3),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        }),
    };
    event_loop.handle_rpc(envelope).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.current_term, Term(leader_term.0 + 3));
}

#[tokio::test]
async fn leader_steps_down_on_fetch_snapshot_request_with_higher_epoch() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let leader_term = event_loop.state.current_term;

    let envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(leader_term.0 + 5),
        source: NodeId(3),
        payload: RpcPayload::FetchSnapshotRequest(FetchSnapshotRequest {
            snapshot_id: SnapshotId {
                end_offset: 10,
                epoch: Term(2),
            },
            position: 0,
            max_bytes: 4096,
        }),
    };
    event_loop.handle_rpc(envelope).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.current_term, Term(leader_term.0 + 5));
}

#[tokio::test]
async fn leader_steps_down_on_fetch_snapshot_response_with_higher_epoch() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let leader_term = event_loop.state.current_term;

    let envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(leader_term.0 + 7),
        source: NodeId(3),
        payload: RpcPayload::FetchSnapshotResponse(xraft_core::rpc::FetchSnapshotResponse {
            snapshot_id: SnapshotId {
                end_offset: 10,
                epoch: Term(2),
            },
            position: 0,
            data: bytes::Bytes::new(),
            is_last_chunk: true,
        }),
    };
    event_loop.handle_rpc(envelope).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.current_term, Term(leader_term.0 + 7));
}

#[tokio::test]
async fn leader_steps_down_on_vote_response_with_higher_term() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let leader_term = event_loop.state.current_term;

    // A VoteResponse with a higher term should also trigger step-down
    let envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(3),
        payload: RpcPayload::VoteResponse(xraft_core::rpc::VoteResponse {
            term: Term(leader_term.0 + 10),
            vote_granted: false,
            is_pre_vote: false,
        }),
    };
    event_loop.handle_rpc(envelope).await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.current_term, Term(leader_term.0 + 10));
}

// ═══════════════════════════════════════════════════════════════════
// Test: Fetch liveness uses envelope source, not req.replica_id
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn fetch_records_envelope_source_not_replica_id() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Send a FetchRequest where envelope.source=N2 but req.replica_id=N3.
    // Only N2 (the real sender) should get its timestamp updated.
    let envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: event_loop.state.current_term,
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(3), // spoofed replica_id
            fetch_offset: 42,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    event_loop.handle_rpc(envelope).await.unwrap();

    // N2 should have its timestamp set (from envelope.source)
    let n2_progress = event_loop.state.follower_state.get(&NodeId(2)).unwrap();
    assert!(
        n2_progress.last_fetch_timestamp.is_some(),
        "N2 (envelope source) should have its fetch timestamp recorded"
    );
    assert_eq!(n2_progress.fetch_offset, 42);

    // N3 should still have None (never actually fetched)
    let n3_progress = event_loop.state.follower_state.get(&NodeId(3)).unwrap();
    assert!(
        n3_progress.last_fetch_timestamp.is_none(),
        "N3 should NOT have its timestamp updated by a spoofed replica_id"
    );
}

// ═══════════════════════════════════════════════════════════════════
// Test: handle_check_quorum via event loop
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn handle_check_quorum_resets_deadline_on_pass() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // Record recent fetch from N2 via handle_rpc
    let fetch_env = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: event_loop.state.current_term,
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(2),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    event_loop.handle_rpc(fetch_env).await.unwrap();

    // Advance to check_quorum_deadline
    let election_timeout = Duration::from_millis(150);
    clock.advance(election_timeout);
    event_loop.state.check_quorum_deadline = clock.current();

    let deadline_before = event_loop.state.check_quorum_deadline;
    event_loop.handle_check_quorum().await.unwrap();

    assert_eq!(event_loop.state.role, Role::Leader);
    // Deadline should have been reset forward
    assert!(event_loop.state.check_quorum_deadline > deadline_before);
}

#[tokio::test]
async fn handle_check_quorum_steps_down_on_fail() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;

    // No fetches recorded — advance past election timeout
    let election_timeout = Duration::from_millis(150);
    clock.advance(election_timeout + Duration::from_millis(10));
    event_loop.state.check_quorum_deadline = clock.current();

    event_loop.handle_check_quorum().await.unwrap();

    assert_eq!(event_loop.state.role, Role::Follower);
    assert_eq!(event_loop.state.leader_id, None);
    assert_eq!(event_loop.state.voted_for, Some(NodeId(1)),
        "voted_for must be preserved on check-quorum step-down");
}

// ═══════════════════════════════════════════════════════════════════
// Integration tests: full event loop run() with timer-driven flows
// ═══════════════════════════════════════════════════════════════════

/// Helper: wait for a specific role to appear in the state watch channel.
/// Panics if the role is not observed within `max_attempts` yield cycles.
async fn wait_for_role(
    state_rx: &mut watch::Receiver<xraft_core::ConsensusState>,
    expected: Role,
    label: &str,
) -> xraft_core::ConsensusState {
    for _ in 0..20 {
        let _ = tokio::time::timeout(Duration::from_millis(50), state_rx.changed()).await;
        let snap = state_rx.borrow().clone();
        if snap.role == expected {
            return snap;
        }
        tokio::task::yield_now().await;
    }
    let snap = state_rx.borrow().clone();
    panic!("expected role {label} ({expected:?}) but got {:?}", snap.role);
}

/// Drive the event loop via `run()` in a background task, using the
/// message channel and simulated clock to orchestrate a full
/// election → leader → check-quorum-pass flow.
#[tokio::test]
async fn event_loop_run_election_and_check_quorum_pass() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, mut state_rx, listener) = build_event_loop(&clock, &log_store);

    let now = clock.current();
    event_loop
        .state
        .become_follower(Term(0), None, now + Duration::from_millis(150));

    let clock_clone = clock.clone();

    let handle = tokio::spawn(async move {
        let _ = event_loop.run().await;
    });

    // Advance past election deadline to trigger election
    clock_clone.advance(Duration::from_millis(160));
    let snap = wait_for_role(&mut state_rx, Role::Candidate, "Candidate after election timeout").await;

    let vote_resp = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(2),
        payload: RpcPayload::VoteResponse(xraft_core::rpc::VoteResponse {
            term: snap.current_term,
            vote_granted: true,
            is_pre_vote: false,
        }),
    };
    msg_tx.send(EventLoopMessage::Rpc(vote_resp)).await.unwrap();

    let snap = wait_for_role(&mut state_rx, Role::Leader, "Leader after vote").await;
    assert_eq!(snap.role, Role::Leader);

    // Send Fetch from N2 so quorum holds
    let fetch = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: snap.current_term,
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(2),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    msg_tx.send(EventLoopMessage::Rpc(fetch)).await.unwrap();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Advance only 100ms — less than election_timeout (150ms) so the
    // fetch is still fresh when the check-quorum runs.
    clock_clone.advance(Duration::from_millis(100));
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Now send another fetch right before the deadline fires, keeping quorum alive
    let fetch2 = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: snap.current_term,
        source: NodeId(2),
        payload: RpcPayload::FetchRequest(FetchRequest {
            replica_id: NodeId(2),
            fetch_offset: 1,
            last_fetched_epoch: Term(0),
            max_bytes: 1024,
        }),
    };
    msg_tx.send(EventLoopMessage::Rpc(fetch2)).await.unwrap();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Now advance past the check-quorum deadline (150ms from leader election)
    clock_clone.advance(Duration::from_millis(60));
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Give time for the check-quorum to run; leader should remain
    let _ = tokio::time::timeout(Duration::from_millis(50), state_rx.changed()).await;
    let snap = state_rx.borrow().clone();
    assert_eq!(snap.role, Role::Leader);

    // Verify LeaderChangeMessage was persisted
    let entries = log_store.entries();
    assert!(
        entries.iter().any(|e| e.entry_type == EntryType::LeaderChangeMessage),
        "LeaderChangeMessage should be persisted in log store"
    );
    let changes = listener.leader_changes();
    assert!(!changes.is_empty());

    msg_tx.send(EventLoopMessage::Shutdown).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
}

/// Drive the event loop via `run()` to test check-quorum failure.
#[tokio::test]
async fn event_loop_run_check_quorum_fail_steps_down() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, mut state_rx, _listener) = build_event_loop(&clock, &log_store);

    let now = clock.current();
    event_loop.state.become_follower(Term(0), None, now + Duration::from_millis(150));

    let clock_clone = clock.clone();

    let handle = tokio::spawn(async move {
        let _ = event_loop.run().await;
    });

    clock_clone.advance(Duration::from_millis(160));
    let snap = wait_for_role(&mut state_rx, Role::Candidate, "Candidate after election timeout").await;

    let vote_resp = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(2),
        payload: RpcPayload::VoteResponse(xraft_core::rpc::VoteResponse {
            term: snap.current_term,
            vote_granted: true,
            is_pre_vote: false,
        }),
    };
    msg_tx.send(EventLoopMessage::Rpc(vote_resp)).await.unwrap();

    let _snap = wait_for_role(&mut state_rx, Role::Leader, "Leader after vote").await;

    // No Fetch sent. Advance past check-quorum deadline.
    clock_clone.advance(Duration::from_millis(160));
    let snap = wait_for_role(&mut state_rx, Role::Follower, "Follower after quorum fail").await;
    assert_eq!(snap.role, Role::Follower);
    assert_eq!(snap.leader_id, None);

    msg_tx.send(EventLoopMessage::Shutdown).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
}

/// Single-node cluster becomes leader immediately on election timeout.
#[tokio::test]
async fn event_loop_run_single_node_immediate_leader() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());

    let config = test_config();
    let now = clock.current();
    let voters = vec![VoterInfo {
        node_id: NodeId(1),
        endpoint: "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
    }];
    let mut state = NodeState::new(NodeId(1), test_cluster_id(), voters, now);
    state.become_follower(Term(0), None, now + Duration::from_millis(150));

    let io_stage = IoStage::new(
        Box::new(ArcLogStore(Arc::clone(&log_store))),
        Box::new(NullTransport),
        Box::new(InMemoryQuorumStateStore::new()),
        Box::new(NullSnapshotIO),
    );

    let listener = RecordingListener::new();
    let (msg_tx, msg_rx) = mpsc::channel(64);
    let initial = state.project();
    let (state_tx, mut state_rx) = watch::channel(initial);

    let listener_clone = listener.clone();
    let mut event_loop = EventLoop::new(
        state, config, Box::new(clock.clone()), io_stage,
        NullStateMachine, listener_clone, msg_rx, state_tx,
    );

    let clock_clone = clock.clone();
    let handle = tokio::spawn(async move {
        let _ = event_loop.run().await;
    });

    clock_clone.advance(Duration::from_millis(160));
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let _ = tokio::time::timeout(Duration::from_millis(50), state_rx.changed()).await;
    let snap = state_rx.borrow().clone();
    assert_eq!(snap.role, Role::Leader, "single-node should become leader immediately");

    let entries = log_store.entries();
    assert!(
        entries.iter().any(|e| e.entry_type == EntryType::LeaderChangeMessage),
        "LeaderChangeMessage should be appended for single-node leader"
    );

    let changes = listener.leader_changes();
    assert!(!changes.is_empty());

    msg_tx.send(EventLoopMessage::Shutdown).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
}

/// Leader receives higher-term message during run(), steps down.
#[tokio::test]
async fn event_loop_run_higher_term_step_down() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, mut state_rx, _listener) = build_event_loop(&clock, &log_store);

    let now = clock.current();
    event_loop.state.become_follower(Term(0), None, now + Duration::from_millis(150));

    let clock_clone = clock.clone();

    let handle = tokio::spawn(async move {
        let _ = event_loop.run().await;
    });

    clock_clone.advance(Duration::from_millis(160));
    let snap = wait_for_role(&mut state_rx, Role::Candidate, "Candidate after election timeout").await;

    let vote_resp = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(2),
        payload: RpcPayload::VoteResponse(xraft_core::rpc::VoteResponse {
            term: snap.current_term,
            vote_granted: true,
            is_pre_vote: false,
        }),
    };
    msg_tx.send(EventLoopMessage::Rpc(vote_resp)).await.unwrap();

    let snap = wait_for_role(&mut state_rx, Role::Leader, "Leader after vote").await;
    let leader_term = snap.current_term;

    let higher_term_msg = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(leader_term.0 + 20),
        source: NodeId(3),
        payload: RpcPayload::FetchSnapshotResponse(xraft_core::rpc::FetchSnapshotResponse {
            snapshot_id: SnapshotId {
                end_offset: 5,
                epoch: Term(1),
            },
            position: 0,
            data: bytes::Bytes::new(),
            is_last_chunk: true,
        }),
    };
    msg_tx.send(EventLoopMessage::Rpc(higher_term_msg)).await.unwrap();

    let snap = wait_for_role(&mut state_rx, Role::Follower, "Follower after higher-term msg").await;
    assert_eq!(snap.role, Role::Follower);
    assert_eq!(snap.current_term, Term(leader_term.0 + 20));

    msg_tx.send(EventLoopMessage::Shutdown).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
}

// ═══════════════════════════════════════════════════════════════════
// Test: Pre-vote must NOT mutate term or voted_for (Stage 4.3 rule)
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn pre_vote_request_does_not_mutate_state() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, _msg_tx, _, _) = build_event_loop(&clock, &log_store);

    // Start as Follower in term 1, voted_for = None
    let now = clock.current();
    event_loop.state.become_follower(Term(1), None, now + Duration::from_millis(300));
    assert_eq!(event_loop.state.current_term, Term(1));
    assert_eq!(event_loop.state.voted_for, None);

    let term_before = event_loop.state.current_term;
    let voted_for_before = event_loop.state.voted_for;
    let role_before = event_loop.state.role;

    // Receive a pre-vote VoteRequest with a HIGHER term
    let pre_vote_envelope = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(3),
        payload: RpcPayload::VoteRequest(xraft_core::rpc::VoteRequest {
            term: Term(5),
            candidate_id: NodeId(3),
            last_log_offset: 0,
            last_log_term: Term(0),
            is_pre_vote: true,
        }),
    };
    event_loop.handle_rpc(pre_vote_envelope).await.unwrap();

    // State must remain UNCHANGED: no term bump, no voted_for, no role change
    assert_eq!(event_loop.state.current_term, term_before,
        "pre-vote must not advance current_term");
    assert_eq!(event_loop.state.voted_for, voted_for_before,
        "pre-vote must not set voted_for");
    assert_eq!(event_loop.state.role, role_before,
        "pre-vote must not change role");
}

#[tokio::test]
async fn pre_vote_response_does_not_mutate_state() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let term_before = event_loop.state.current_term;
    let role_before = event_loop.state.role;

    // Receive a pre-vote VoteResponse with a HIGHER term
    let pre_vote_resp = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(3),
        payload: RpcPayload::VoteResponse(xraft_core::rpc::VoteResponse {
            term: Term(term_before.0 + 10),
            vote_granted: false,
            is_pre_vote: true,
        }),
    };
    event_loop.handle_rpc(pre_vote_resp).await.unwrap();

    // Must remain Leader, term unchanged
    assert_eq!(event_loop.state.current_term, term_before,
        "pre-vote response must not advance term");
    assert_eq!(event_loop.state.role, role_before,
        "pre-vote response must not trigger step-down");
}

// ═══════════════════════════════════════════════════════════════════
// Test: LeaderChangeMessage updates last_log_term
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn leader_change_message_updates_last_log_term() {
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    // Before election, last_log_term should be 0
    assert_eq!(event_loop.state.last_log_term, Term(0));

    // Win election for term 5
    make_leader_at_term(&mut event_loop, &msg_tx, &clock, Term(5)).await;

    // After LeaderChangeMessage append, last_log_term must be updated to 5
    assert_eq!(event_loop.state.last_log_term, Term(5),
        "last_log_term must be updated by LeaderChangeMessage append");
    assert_eq!(event_loop.state.log_end_offset, 1);
}

// ═══════════════════════════════════════════════════════════════════
// Test: Leader safety — same-term VoteRequest must be rejected
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn leader_rejects_same_term_vote_request() {
    // A leader must never grant a vote in the same term, because doing so
    // would allow two leaders in one term, violating Raft's safety property.
    let clock = SimulatedClock::new(Duration::from_millis(150));
    let log_store = Arc::new(InMemoryLogStore::new());
    let (mut event_loop, msg_tx, _, _) = build_event_loop(&clock, &log_store);

    make_leader(&mut event_loop, &msg_tx, &clock).await;
    let leader_term = event_loop.state.current_term;
    assert_eq!(event_loop.state.role, Role::Leader);

    // voted_for must be preserved as Some(self) after becoming leader
    assert_eq!(event_loop.state.voted_for, Some(NodeId(1)),
        "leader must retain voted_for=self to prevent same-term vote grants");

    // Send a same-term VoteRequest from another node
    let vote_req = RpcEnvelope {
        cluster_id: test_cluster_id(),
        leader_epoch: Term(0),
        source: NodeId(3),
        payload: RpcPayload::VoteRequest(xraft_core::rpc::VoteRequest {
            term: leader_term,
            candidate_id: NodeId(3),
            last_log_offset: 100,
            last_log_term: leader_term,
            is_pre_vote: false,
        }),
    };
    event_loop.handle_rpc(vote_req).await.unwrap();

    // Must remain Leader — vote must not be granted
    assert_eq!(event_loop.state.role, Role::Leader,
        "leader must not step down on same-term VoteRequest");
    assert_eq!(event_loop.state.current_term, leader_term);
    assert_eq!(event_loop.state.voted_for, Some(NodeId(1)),
        "voted_for must remain self after rejecting same-term vote");
}

// ═══════════════════════════════════════════════════════════════════
// Test: I/O error propagation from run()
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn run_propagates_io_error() {
    // Verify that run() returns Err on I/O failure rather than
    // logging and continuing with inconsistent state.

    let clock = SimulatedClock::new(Duration::from_millis(150));
    let config = test_config();
    let now = clock.current();
    let voters = three_node_voters();
    let mut state = NodeState::new(NodeId(1), test_cluster_id(), voters, now);
    state.become_follower(Term(0), None, now + Duration::from_millis(150));

    // Use a FailingQuorumStore that always errors on save
    let io_stage = IoStage::new(
        Box::new(InMemoryLogStore::new()),
        Box::new(NullTransport),
        Box::new(FailingQuorumStore),
        Box::new(NullSnapshotIO),
    );

    let listener = RecordingListener::new();
    let (msg_tx, msg_rx) = mpsc::channel(64);
    let initial = state.project();
    let (state_tx, _state_rx) = watch::channel(initial);

    let mut event_loop = EventLoop::new(
        state, config, Box::new(clock.clone()), io_stage,
        NullStateMachine, listener.clone(), msg_rx, state_tx,
    );

    let clock_clone = clock.clone();
    let handle = tokio::spawn(async move {
        event_loop.run().await
    });

    // Advance past election deadline → triggers election → PersistQuorumState fails
    clock_clone.advance(Duration::from_millis(160));
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let result = tokio::time::timeout(Duration::from_millis(200), handle).await;
    match result {
        Ok(Ok(Err(_))) => { /* run() returned an error — correct */ }
        Ok(Ok(Ok(()))) => panic!("run() should have returned Err on I/O failure"),
        Ok(Err(e)) => panic!("task panicked: {e}"),
        Err(_) => {
            // Timed out — send shutdown to clean up
            let _ = msg_tx.send(EventLoopMessage::Shutdown).await;
            panic!("run() did not terminate on I/O error within timeout");
        }
    }
}

/// A QuorumStateStore that always fails on save.
struct FailingQuorumStore;

#[async_trait::async_trait]
impl xraft_core::traits::QuorumStateStore for FailingQuorumStore {
    async fn load(&self) -> Result<Option<xraft_core::QuorumState>, std::io::Error> {
        Ok(None)
    }
    async fn save(&self, _state: &xraft_core::QuorumState) -> Result<(), std::io::Error> {
        Err(std::io::Error::other("simulated I/O failure"))
    }
}
