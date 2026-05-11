//! Mock transport implementations for testing.
//!
//! Provides [`MockTransportSender`] and [`MockTransportReceiver`] that
//! implement the `TransportSender` and `TransportReceiver` traits from
//! `xraft_core::traits`, backed by `tokio::sync::mpsc` channels.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use xraft_core::error::XraftError;
use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::{TransportReceiver, TransportSender};

/// A mock sender that pushes [`RpcEnvelope`]s into an in-memory channel.
///
/// Useful for unit tests that need to verify outbound messages without
/// a real network.
pub struct MockTransportSender {
    tx: mpsc::Sender<RpcEnvelope>,
}

impl MockTransportSender {
    /// Create a new mock sender wrapping the given channel sender.
    pub fn new(tx: mpsc::Sender<RpcEnvelope>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl TransportSender for MockTransportSender {
    /// Send an [`RpcEnvelope`] through the backing channel.
    ///
    /// Returns `Err(XraftError::TransportError { .. })` if the
    /// receiver has been dropped.
    async fn send(&self, envelope: RpcEnvelope) -> Result<(), XraftError> {
        self.tx.send(envelope).await.map_err(|e| {
            XraftError::TransportError {
                reason: format!("mock send failed: {e}"),
            }
        })
    }
}

/// A mock receiver that reads [`RpcEnvelope`]s from an in-memory channel.
pub struct MockTransportReceiver {
    rx: Arc<Mutex<mpsc::Receiver<RpcEnvelope>>>,
}

impl MockTransportReceiver {
    /// Create a new mock receiver wrapping the given channel receiver.
    pub fn new(rx: mpsc::Receiver<RpcEnvelope>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

#[async_trait]
impl TransportReceiver for MockTransportReceiver {
    /// Receive the next [`RpcEnvelope`] from the backing channel.
    ///
    /// Returns `Err(XraftError::TransportError { .. })` if the channel
    /// is closed with no remaining messages.
    async fn recv(&self) -> Result<RpcEnvelope, XraftError> {
        self.rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| XraftError::TransportError {
                reason: "mock recv channel closed".to_string(),
            })
    }
}

/// Create a linked `(sender, receiver)` mock transport pair.
///
/// Messages sent through the sender appear on the receiver, useful
/// for wiring up test harnesses without real I/O.
pub fn mock_transport_pair(buffer: usize) -> (MockTransportSender, MockTransportReceiver) {
    let (tx, rx) = mpsc::channel(buffer);
    (MockTransportSender::new(tx), MockTransportReceiver::new(rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_recv_round_trip() {
        let (sender, receiver) = mock_transport_pair(8);
        let envelope = RpcEnvelope::default();
        sender.send(envelope.clone()).await.unwrap();
        let got = receiver.recv().await.unwrap();
        assert_eq!(got, envelope);
    }

    #[tokio::test]
    async fn recv_returns_transport_error_on_closed_channel() {
        let (_sender, receiver) = mock_transport_pair(1);
        drop(_sender);
        let result = receiver.recv().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::TransportError { reason } => {
                assert!(reason.contains("closed"), "unexpected reason: {reason}");
            }
            other => panic!("expected TransportError, got: {other:?}"),
        }
    }
}
