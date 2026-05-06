use crate::config::RaftConfig;
use crate::election::{ElectionManager, ElectionOutput};
use crate::rpc::{RpcEnvelope, RpcPayload};
use crate::traits::Clock;
use crate::types::{ClusterId, NodeId};
use crate::voter::VoterInfo;

/// Events processed by the EventLoop, dispatched to the appropriate manager.
///
/// The EventLoop receives `NodeEvent` values from:
/// - `ReceiverTask` (via mpsc channel): `RpcReceived` events
/// - Timer ticks: `Tick` events (checked each iteration)
///
/// Each event is dispatched to the corresponding manager (ElectionManager,
/// ReplicationManager, MembershipManager) and produces an `ElectionOutput`
/// (or equivalent output type) that the IoStage executes.
#[derive(Debug)]
pub enum NodeEvent {
    /// A timer tick — the EventLoop checks deadlines (election timeout,
    /// check-quorum deadline) on each iteration.
    Tick,

    /// An inbound RPC from a peer node.
    RpcReceived {
        from: NodeId,
        envelope: RpcEnvelope,
    },
}

/// Full internal protocol state for a Raft node.
///
/// Wraps the sub-managers (ElectionManager, and in future ReplicationManager,
/// MembershipManager) and provides event dispatch for the EventLoop.
///
/// The EventLoop owns a `NodeState` and calls `handle_event` for each
/// incoming `NodeEvent`. The returned `ElectionOutput` is then executed
/// by the `IoStage`.
///
/// `NodeState` is `pub(crate)` — the public API surface is `ConsensusState`
/// (a projected subset) exposed via `RaftNode::read()`.
pub struct NodeState {
    election: ElectionManager,
}

impl NodeState {
    pub fn new(
        node_id: NodeId,
        cluster_id: ClusterId,
        voter_set: Vec<VoterInfo>,
        config: RaftConfig,
    ) -> Self {
        Self {
            election: ElectionManager::new(node_id, cluster_id, voter_set, config),
        }
    }

    /// Access the election manager (read-only).
    pub fn election(&self) -> &ElectionManager {
        &self.election
    }

    /// Access the election manager (mutable).
    pub fn election_mut(&mut self) -> &mut ElectionManager {
        &mut self.election
    }

    /// Dispatch a `NodeEvent` to the appropriate handler.
    ///
    /// Returns an `ElectionOutput` containing persistence and send actions
    /// plus state-transition events. The EventLoop passes this to the
    /// `IoStage` for execution.
    ///
    /// **Processing order per the architecture:**
    /// 1. Mutate state (via ElectionManager methods)
    /// 2. Collect events (role changes)
    /// 3. Return IoActions for IoStage execution
    /// 4. (EventLoop updates watch channel with ConsensusState projection)
    /// 5. (IoStage executes persist-then-send)
    pub fn handle_event(&mut self, event: NodeEvent, clock: &dyn Clock) -> ElectionOutput {
        match event {
            NodeEvent::Tick => self.election.tick(clock),

            NodeEvent::RpcReceived { from, envelope } => {
                self.dispatch_rpc(from, envelope, clock)
            }
        }
    }

    /// Route an RPC to the appropriate handler based on payload type.
    fn dispatch_rpc(
        &mut self,
        from: NodeId,
        envelope: RpcEnvelope,
        clock: &dyn Clock,
    ) -> ElectionOutput {
        match envelope.payload {
            RpcPayload::VoteRequest(ref req) => {
                self.election.handle_vote_request(from, req, clock)
            }
            RpcPayload::VoteResponse(ref resp) => {
                self.election.handle_vote_response(from, resp, clock)
            }
        }
    }
}
