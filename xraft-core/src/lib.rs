pub mod error;
pub mod rpc;
pub mod traits;
pub mod types;

pub use error::XraftError;
pub use rpc::{
    AddVoterRequest, DivergingEpoch, FetchRequest, FetchResponse, FetchSnapshotRequest,
    FetchSnapshotResponse, MembershipChangeResponse, MembershipError, RemoveVoterRequest,
    RpcEnvelope, RpcPayload, SnapshotId, UpdateVoterRequest, VoteRequest, VoteResponse,
};
pub use traits::{TransportReceiver, TransportSender};
pub use types::{ClusterId, NodeId, Term};

pub type Result<T> = std::result::Result<T, XraftError>;
