use crate::election::ElectionOutput;
use crate::io_action::IoAction;
use crate::rpc::RpcEnvelope;
use crate::traits::{QuorumStateStore, Result};
use crate::types::NodeId;
use async_trait::async_trait;

/// Executes I/O actions produced by the EventLoop.
///
/// The `IoStage` is **owned** by the event loop task and enforces the
/// fsync-before-ack contract: `PersistQuorumState` actions are awaited
/// and must succeed before any `SendRpc` action is dispatched. If
/// persistence fails, no RPCs are sent and the error propagates to the
/// event loop.
///
/// The `IoStage` holds owned trait objects for the injected I/O
/// implementations (`QuorumStateStore`, `TransportSender`).
pub struct IoStage {
    quorum_store: Box<dyn QuorumStateStore>,
    transport: Box<dyn TransportSender>,
}

/// Trait for sending RPCs to peers. Owned by the IoStage.
///
/// Takes `&self` (shared reference) because the IoStage may send to
/// multiple peers concurrently. Requires `Send + Sync + 'static`.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

impl IoStage {
    pub fn new(
        quorum_store: Box<dyn QuorumStateStore>,
        transport: Box<dyn TransportSender>,
    ) -> Self {
        Self {
            quorum_store,
            transport,
        }
    }

    /// Execute an `ElectionOutput` with strict ordering:
    /// 1. Execute ALL `persist_first` actions (PersistQuorumState).
    ///    If any fails, return error immediately — no RPCs are sent.
    /// 2. Execute ALL `then_send` actions (SendRpc) after persistence.
    ///
    /// This is the production path for enforcing fsync-before-ack.
    pub async fn execute_election_output(&self, output: ElectionOutput) -> Result<()> {
        // Phase 1: persist (must complete before sends)
        for action in output.persist_first.actions {
            match action {
                IoAction::PersistQuorumState(qs) => {
                    self.quorum_store.save(&qs).await?;
                }
                IoAction::SendRpc(_, _) => {
                    // persist_first should never contain SendRpc,
                    // but if it does, treat as programming error.
                    panic!("persist_first must not contain SendRpc");
                }
            }
        }

        // Phase 2: send RPCs (only after persistence succeeded)
        for action in output.then_send.actions {
            match action {
                IoAction::SendRpc(target, envelope) => {
                    self.transport.send(target, envelope).await?;
                }
                IoAction::PersistQuorumState(_) => {
                    panic!("then_send must not contain PersistQuorumState");
                }
            }
        }

        Ok(())
    }

    /// Convert an ElectionOutput into an ordered list of IoActions.
    /// Persist actions come first, then send actions.
    ///
    /// This is a convenience for tests and diagnostics — the production
    /// path should use `execute_election_output` which enforces the
    /// await boundary between phases.
    pub fn ordered_actions(output: ElectionOutput) -> Vec<IoAction> {
        let mut actions = Vec::new();
        for action in output.persist_first.actions {
            actions.push(action);
        }
        for action in output.then_send.actions {
            actions.push(action);
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::XraftError;
    use crate::io_action::IoActionBatch;
    use crate::quorum_state::QuorumState;
    use crate::rpc::{RpcEnvelope, RpcPayload, VoteResponse};
    use crate::types::{ClusterId, NodeId, Term};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct MockQuorumStore {
        saved: Mutex<Vec<QuorumState>>,
        fail_on_save: AtomicBool,
    }

    impl MockQuorumStore {
        fn new() -> Self {
            Self {
                saved: Mutex::new(Vec::new()),
                fail_on_save: AtomicBool::new(false),
            }
        }

        fn set_fail(&self, fail: bool) {
            self.fail_on_save.store(fail, Ordering::SeqCst);
        }

        fn save_count(&self) -> usize {
            self.saved.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl QuorumStateStore for MockQuorumStore {
        async fn load(&self) -> Result<Option<QuorumState>> {
            Ok(None)
        }

        async fn save(&self, state: &QuorumState) -> Result<()> {
            if self.fail_on_save.load(Ordering::SeqCst) {
                return Err(XraftError::StorageError("fsync failed".into()));
            }
            self.saved.lock().unwrap().push(state.clone());
            Ok(())
        }
    }

    struct MockTransport {
        sent: Mutex<Vec<(NodeId, RpcEnvelope)>>,
        send_count: AtomicUsize,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
                send_count: AtomicUsize::new(0),
            }
        }

        fn send_count(&self) -> usize {
            self.send_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TransportSender for MockTransport {
        async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()> {
            self.send_count.fetch_add(1, Ordering::SeqCst);
            self.sent.lock().unwrap().push((target, message));
            Ok(())
        }
    }

    fn make_test_output() -> ElectionOutput {
        let mut persist = IoActionBatch::new();
        persist.push(IoAction::PersistQuorumState(QuorumState {
            current_term: Term(3),
            voted_for: Some(NodeId(2)),
            leader_id: None,
            leader_epoch: 0,
        }));

        let mut send = IoActionBatch::new();
        let envelope = RpcEnvelope {
            cluster_id: ClusterId(uuid::Uuid::nil()),
            leader_epoch: 0,
            source: NodeId(1),
            payload: RpcPayload::VoteResponse(VoteResponse {
                term: Term(3),
                vote_granted: true,
                is_pre_vote: false,
            }),
        };
        send.push(IoAction::SendRpc(NodeId(2), envelope));

        ElectionOutput {
            persist_first: persist,
            then_send: send,
            events: Vec::new(),
        }
    }

    #[tokio::test]
    async fn test_io_stage_persist_then_send_success() {
        let store = Arc::new(MockQuorumStore::new());
        let transport = Arc::new(MockTransport::new());

        let stage = IoStage::new(
            Box::new(ArcStore(store.clone())),
            Box::new(ArcTransport(transport.clone())),
        );

        let output = make_test_output();
        let result = stage.execute_election_output(output).await;
        assert!(result.is_ok());

        // Persist happened
        assert_eq!(store.save_count(), 1);
        // Send happened
        assert_eq!(transport.send_count(), 1);
    }

    #[tokio::test]
    async fn test_io_stage_persist_failure_prevents_send() {
        let store = Arc::new(MockQuorumStore::new());
        let transport = Arc::new(MockTransport::new());

        store.set_fail(true);

        let stage = IoStage::new(
            Box::new(ArcStore(store.clone())),
            Box::new(ArcTransport(transport.clone())),
        );

        let output = make_test_output();
        let result = stage.execute_election_output(output).await;

        // Persist failed
        assert!(result.is_err());
        // Send was NOT attempted
        assert_eq!(transport.send_count(), 0);
    }

    // Wrappers for Arc-based trait objects
    struct ArcStore(Arc<MockQuorumStore>);
    #[async_trait]
    impl QuorumStateStore for ArcStore {
        async fn load(&self) -> Result<Option<QuorumState>> {
            self.0.load().await
        }
        async fn save(&self, state: &QuorumState) -> Result<()> {
            self.0.save(state).await
        }
    }

    struct ArcTransport(Arc<MockTransport>);
    #[async_trait]
    impl TransportSender for ArcTransport {
        async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()> {
            self.0.send(target, message).await
        }
    }
}
