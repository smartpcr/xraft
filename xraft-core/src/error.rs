use thiserror::Error;

#[derive(Debug, Error)]
pub enum XraftError {
    #[error("storage error: {0}")]
    StorageError(String),

    #[error("transport error: {0}")]
    TransportError(String),

    #[error("not leader")]
    NotLeader,

    #[error("proposal queue full")]
    ProposalQueueFull,

    #[error("invalid cluster id")]
    InvalidClusterId,

    #[error("shutdown")]
    Shutdown,

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}
