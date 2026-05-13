// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// -----------------------------------------------------------------------

//! Crate-wide error and result types.

use std::io;

use thiserror::Error;

/// Convenience alias for `Result<T, RaftError>` used throughout the crate.
pub type RaftResult<T> = Result<T, RaftError>;

/// Errors that can be produced by storage, transport, runtime, or core
/// protocol operations.
///
/// Trait implementors should map their domain-specific failures into one of
/// these variants so that higher layers can react uniformly.
#[derive(Debug, Error)]
pub enum RaftError {
    /// Underlying I/O failure (disk, network, etc).
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// A storage backend reported an error that is not better classified
    /// elsewhere.
    #[error("storage error: {0}")]
    Storage(String),

    /// A transport backend reported an error (connection lost, decode
    /// failure, peer unreachable, etc).
    #[error("transport error: {0}")]
    Transport(String),

    /// The async runtime reported an error (task panicked, executor shut
    /// down, timer dropped, etc).
    #[error("runtime error: {0}")]
    Runtime(String),

    /// A log entry, snapshot, or message could not be serialised or
    /// deserialised.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A requested log index is below the compacted prefix or beyond the
    /// last index.
    #[error("log index {index} out of range (first = {first}, last = {last})")]
    LogIndexOutOfRange {
        /// The requested index.
        index: u64,
        /// First (oldest) index currently retained.
        first: u64,
        /// Last (newest) index currently retained.
        last: u64,
    },

    /// The persisted voter set is inconsistent with the log or snapshot.
    #[error("inconsistent voter set: {0}")]
    InconsistentVoterSet(String),

    /// The peer identifier supplied is not known to this node.
    #[error("unknown peer: {0}")]
    UnknownPeer(String),

    /// The operation was cancelled or the node is shutting down.
    #[error("operation cancelled")]
    Cancelled,

    /// A generic error variant for cases that do not fit elsewhere.
    #[error("{0}")]
    Other(String),
}

impl RaftError {
    /// Build a [`RaftError::Storage`] with a formatted message.
    pub fn storage(msg: impl Into<String>) -> Self {
        RaftError::Storage(msg.into())
    }

    /// Build a [`RaftError::Transport`] with a formatted message.
    pub fn transport(msg: impl Into<String>) -> Self {
        RaftError::Transport(msg.into())
    }

    /// Build a [`RaftError::Runtime`] with a formatted message.
    pub fn runtime(msg: impl Into<String>) -> Self {
        RaftError::Runtime(msg.into())
    }
}
