pub mod app_record;
pub mod error;
pub mod follower_progress;
pub mod log_entry;
pub mod rpc;
pub mod snapshot;
pub mod traits;
pub mod types;

// Re-exports for convenience
pub use app_record::{AppRecord, AppSnapshot};
pub use error::XraftError;
pub use log_entry::{EntryType, LogEntry};
pub use rpc::{
    DivergingEpoch, FetchRequest, FetchResponse, RpcEnvelope, RpcPayload, SnapshotId,
    VoteRequest, VoteResponse,
};
pub use traits::{TransportReceiver, TransportSender};
pub use types::{ClusterId, NodeId, Offset, Term};
pub use voter::{VoterInfo, VotersRecord};
