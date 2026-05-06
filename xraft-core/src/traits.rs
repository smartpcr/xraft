use crate::app_record::{AppRecord, AppSnapshot};
use crate::error::Result;
use crate::log_entry::LogEntry;
use crate::rpc::RpcEnvelope;
use crate::snapshot::{Snapshot, SnapshotWriter};
use crate::types::{NodeId, Term};
use crate::rpc::SnapshotId;
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::Instant;

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
