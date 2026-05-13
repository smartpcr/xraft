use thiserror::Error;

/// Top-level error type for xraft.
///
/// Stage 1.5 of the implementation plan defines the foundational set of
/// variants used by the consensus engine. Later stages may extend this enum
/// with additional variants as new failure modes are introduced (e.g.,
/// invalid-config, recovery-required); those additions belong to the stages
/// that introduce them, not to Stage 1.5.
///
/// The `#[derive(Error)]` macro from the `thiserror` crate implements both
/// [`std::error::Error`] and [`std::fmt::Display`] for the enum based on the
/// `#[error("...")]` attributes attached to each variant.
#[derive(Debug, Error)]
pub enum XraftError {
    /// Failure originating from the storage layer (segment log, snapshot IO,
    /// quorum-state store). The payload is a human-readable description.
    #[error("storage error: {0}")]
    StorageError(String),

    /// Failure originating from the transport layer (TCP send/recv, RPC
    /// codec, channel transport). The payload is a human-readable description.
    #[error("transport error: {0}")]
    TransportError(String),

    /// Returned when a write/propose operation is attempted on a node that is
    /// not currently the leader of the cluster.
    #[error("not leader")]
    NotLeader,

    /// Returned when the leader's proposal queue cannot accept additional
    /// commands and the caller should retry later (back-pressure signal).
    #[error("proposal queue full")]
    ProposalQueueFull,

    /// Returned when an inbound RPC envelope carries a `cluster_id` that does
    /// not match the receiving node's configured cluster identity.
    #[error("invalid cluster id")]
    InvalidClusterId,

    /// Returned when an operation is cancelled because the node is shutting
    /// down. This is a normal lifecycle signal, not a fault.
    #[error("shutdown")]
    Shutdown,
}

/// Convenience [`Result`] alias parameterised by [`XraftError`].
pub type Result<T> = std::result::Result<T, XraftError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn display_matches_variant() {
        assert_eq!(
            XraftError::StorageError("disk full".to_string()).to_string(),
            "storage error: disk full"
        );
        assert_eq!(
            XraftError::TransportError("connection reset".to_string()).to_string(),
            "transport error: connection reset"
        );
        assert_eq!(XraftError::NotLeader.to_string(), "not leader");
        assert_eq!(
            XraftError::ProposalQueueFull.to_string(),
            "proposal queue full"
        );
        assert_eq!(
            XraftError::InvalidClusterId.to_string(),
            "invalid cluster id"
        );
        assert_eq!(XraftError::Shutdown.to_string(), "shutdown");
    }

    #[test]
    fn implements_std_error() {
        // Box<dyn std::error::Error> requires both Display + Error. This
        // compiles only if XraftError implements std::error::Error.
        let err: Box<dyn std::error::Error> =
            Box::new(XraftError::TransportError("timeout".into()));
        assert!(err.source().is_none());
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn variants_are_exhaustive() {
        // Documents the canonical Stage 1.5 variant set; if a variant is added
        // or removed, this match must be updated, alerting reviewers.
        fn covered(e: &XraftError) -> &'static str {
            match e {
                XraftError::StorageError(_) => "storage",
                XraftError::TransportError(_) => "transport",
                XraftError::NotLeader => "not_leader",
                XraftError::ProposalQueueFull => "proposal_queue_full",
                XraftError::InvalidClusterId => "invalid_cluster_id",
                XraftError::Shutdown => "shutdown",
            }
        }
        assert_eq!(covered(&XraftError::NotLeader), "not_leader");
        assert_eq!(covered(&XraftError::Shutdown), "shutdown");
    }
}
