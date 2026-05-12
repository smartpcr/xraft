//! Simulated network layer for deterministic message delivery.
//!
//! Provides a `MessageBus` that buffers `RpcEnvelope` messages and a
//! `SimulatedTransportSender` that implements the `TransportSender` trait
//! by routing through the shared bus. This proves the transport contract
//! works correctly with the consensus engine while maintaining deterministic
//! message ordering for scenario tests.

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::TransportSender;
use xraft_core::types::NodeId;

/// Shared message bus for deterministic message delivery between nodes.
///
/// Messages are buffered and delivered in discrete phases by the
/// `SimulatedCluster` orchestrator, enabling deterministic control over
/// message ordering, partitions, and node failures.
#[derive(Debug, Clone)]
pub struct MessageBus {
    inner: Arc<Mutex<Vec<(NodeId, RpcEnvelope)>>>,
}

impl MessageBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Push a message destined for `target` onto the bus.
    pub fn push(&self, target: NodeId, envelope: RpcEnvelope) {
        self.inner.lock().unwrap().push((target, envelope));
    }

    /// Drain all pending messages from the bus in FIFO order.
    pub fn drain(&self) -> Vec<(NodeId, RpcEnvelope)> {
        std::mem::take(&mut self.inner.lock().unwrap())
    }

    /// Check if the bus has no pending messages.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Transport sender that routes messages through a shared `MessageBus`.
///
/// Implements the `TransportSender` trait to prove the interface contract
/// works correctly in integration scenarios. The `send` future completes
/// immediately (single-poll) because the bus uses `std::sync::Mutex`.
pub struct SimulatedTransportSender {
    source: NodeId,
    bus: MessageBus,
}

impl SimulatedTransportSender {
    pub fn new(source: NodeId, bus: MessageBus) -> Self {
        Self { source, bus }
    }

    pub fn source(&self) -> NodeId {
        self.source
    }
}

#[async_trait]
impl TransportSender for SimulatedTransportSender {
    async fn send(
        &self,
        target: NodeId,
        message: RpcEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.bus.push(target, message);
        Ok(())
    }
}
