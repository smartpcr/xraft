use thiserror::Error;

#[derive(Debug, Error)]
pub enum XraftError {
    #[error("storage error: {0}")]
    StorageError(String),

    #[error("not found")]
    NotFound,

    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

pub type Result<T> = std::result::Result<T, XraftError>;
