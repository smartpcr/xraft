use crate::rpc::RpcEnvelope;
use crate::types::NodeId;
use crate::Result;
use async_trait::async_trait;

/// Outbound RPC transport. Called by IoStage via SendRpc action.
/// Takes `&self` (shared reference) because the IoStage may send to
/// multiple peers concurrently. Requires `Send + Sync + 'static`.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

/// Inbound RPC transport. Called exclusively by ReceiverTask (§4.4).
/// Takes `&mut self` (exclusive access) because only the ReceiverTask
/// reads from the network. Requires `Send + 'static`.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    async fn recv(&mut self) -> Result<RpcEnvelope>;
}
