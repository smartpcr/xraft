use std::sync::Mutex;

use async_trait::async_trait;
use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::{TransportReceiver, TransportSender};
use xraft_core::types::NodeId;

/// Captured outbound message with its target.
#[derive(Debug, Clone)]
pub struct SentMessage {
    pub target: NodeId,
    pub envelope: RpcEnvelope,
}

/// Transport sender that captures all sent messages for test assertions.
pub struct MockTransportSender {
    pub sent: Mutex<Vec<SentMessage>>,
}

impl MockTransportSender {
    pub fn new() -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
        }
    }

    pub fn take_sent(&self) -> Vec<SentMessage> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

impl Default for MockTransportSender {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransportSender for MockTransportSender {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> std::io::Result<()> {
        self.sent.lock().unwrap().push(SentMessage {
            target,
            envelope: message,
        });
        Ok(())
    }
}

pub struct MockTransportReceiver;

#[async_trait]
impl TransportReceiver for MockTransportReceiver {
    async fn recv(&mut self) -> std::io::Result<RpcEnvelope> {
        // Block forever for tests — never returns a message
        tokio::sync::Notify::new().notified().await;
        unreachable!()
    }
}
