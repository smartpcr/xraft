use async_trait::async_trait;
use bytes::Bytes;

use crate::rpc::RpcEnvelope;
use crate::types::NodeId;

/// Outbound RPC transport. Takes `&self` for concurrent sends by IoStage.
/// Must be `Send + Sync + 'static`.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<(), crate::error::XraftError>;
}

/// Inbound RPC transport. Takes `&mut self` for exclusive access by ReceiverTask.
/// Must be `Send + 'static`.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    async fn recv(&mut self) -> Result<RpcEnvelope, crate::error::XraftError>;
}
