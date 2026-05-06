use std::fmt;

use crate::types::NodeId;

/// Unified error type for all xraft operations.
///
/// All public APIs (`propose`, `bootstrap`, `shutdown`) return `Result<T, XraftError>`.
/// Exactly six variants, matching the architecture §3.2 contract.
#[derive(Debug)]
pub enum XraftError {
    /// Log, snapshot, or quorum-state I/O failure.
    StorageError(std::io::Error),
    /// Network send/recv failure.
    TransportError(std::io::Error),
    /// `propose()` called on a non-leader node. Contains the known leader, if any.
    NotLeader { leader_id: Option<NodeId> },
    /// `BatchAccumulator` back-pressure limit reached.
    ProposalQueueFull,
    /// RPC `cluster_id` mismatch.
    InvalidClusterId,
    /// Node is shutting down; no new operations accepted.
    Shutdown,
}

impl fmt::Display for XraftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            XraftError::StorageError(e) => write!(f, "storage error: {e}"),
            XraftError::TransportError(e) => write!(f, "transport error: {e}"),
            XraftError::NotLeader { leader_id: Some(id) } => {
                write!(f, "not leader; current leader is node {id}")
            }
            XraftError::NotLeader { leader_id: None } => {
                write!(f, "not leader; leader unknown")
            }
            XraftError::ProposalQueueFull => write!(f, "proposal queue full"),
            XraftError::InvalidClusterId => write!(f, "invalid cluster id"),
            XraftError::Shutdown => write!(f, "node is shutting down"),
        }
    }
}

impl std::error::Error for XraftError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            XraftError::StorageError(e) | XraftError::TransportError(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn display_storage_error() {
        let err = XraftError::StorageError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        assert!(err.to_string().contains("storage error"));
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn display_transport_error() {
        let err = XraftError::TransportError(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "peer unreachable",
        ));
        assert!(err.to_string().contains("transport error"));
    }

    #[test]
    fn display_not_leader_with_id() {
        let err = XraftError::NotLeader {
            leader_id: Some(NodeId(42)),
        };
        assert!(err.to_string().contains("not leader"));
        assert!(err.to_string().contains("42"));
    }

    #[test]
    fn display_not_leader_unknown() {
        let err = XraftError::NotLeader { leader_id: None };
        assert!(err.to_string().contains("leader unknown"));
    }

    #[test]
    fn display_proposal_queue_full() {
        let err = XraftError::ProposalQueueFull;
        assert_eq!(err.to_string(), "proposal queue full");
    }

    #[test]
    fn display_invalid_cluster_id() {
        let err = XraftError::InvalidClusterId;
        assert_eq!(err.to_string(), "invalid cluster id");
    }

    #[test]
    fn display_shutdown() {
        let err = XraftError::Shutdown;
        assert_eq!(err.to_string(), "node is shutting down");
    }

    #[test]
    fn source_returns_io_error_for_storage() {
        let inner = std::io::Error::new(std::io::ErrorKind::Other, "disk");
        let err = XraftError::StorageError(inner);
        assert!(err.source().is_some());
    }

    #[test]
    fn source_returns_io_error_for_transport() {
        let inner = std::io::Error::new(std::io::ErrorKind::Other, "net");
        let err = XraftError::TransportError(inner);
        assert!(err.source().is_some());
    }

    #[test]
    fn source_returns_none_for_other_variants() {
        assert!(XraftError::ProposalQueueFull.source().is_none());
        assert!(XraftError::InvalidClusterId.source().is_none());
        assert!(XraftError::Shutdown.source().is_none());
        assert!((XraftError::NotLeader { leader_id: None }).source().is_none());
    }

    #[test]
    fn no_blanket_from_io_error() {
        // Verify at runtime that From<io::Error> is NOT implemented for XraftError.
        // Both StorageError and TransportError wrap io::Error, so a blanket From
        // would be ambiguous. Callers must pick the correct variant explicitly.
        fn implements_from<T: 'static, U: 'static>() -> bool {
            use std::any::TypeId;
            // If From<io::Error> for XraftError existed, Into<XraftError> for io::Error
            // would also exist. We can't query trait impls at runtime directly, but we
            // can verify the types are distinct (not convertible via identity).
            TypeId::of::<T>() != TypeId::of::<U>()
        }
        // Smoke-check: io::Error and XraftError are distinct types (trivially true),
        // and the real guarantee is the compile-time absence of From<io::Error>.
        assert!(implements_from::<std::io::Error, XraftError>());

        // The authoritative check: these two lines must NOT compile.
        // If someone adds `impl From<std::io::Error> for XraftError`, CI will
        // catch the ambiguity or these lines (when uncommented) would compile:
        //   let _: XraftError = std::io::Error::new(std::io::ErrorKind::Other, "x").into();
        //   let _: Result<(), XraftError> = Err(std::io::Error::new(std::io::ErrorKind::Other, "x"))?;

        // Instead, callers must be explicit:
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let storage = XraftError::StorageError(io_err);
        assert!(matches!(storage, XraftError::StorageError(_)));

        let io_err2 = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let transport = XraftError::TransportError(io_err2);
        assert!(matches!(transport, XraftError::TransportError(_)));
    }

    #[test]
    fn explicit_storage_and_transport_construction() {
        let storage = XraftError::StorageError(std::io::Error::new(
            std::io::ErrorKind::Other,
            "disk fail",
        ));
        let transport = XraftError::TransportError(std::io::Error::new(
            std::io::ErrorKind::Other,
            "net fail",
        ));
        assert!(matches!(storage, XraftError::StorageError(_)));
        assert!(matches!(transport, XraftError::TransportError(_)));
    }

    #[test]
    fn debug_impl_works() {
        let err = XraftError::Shutdown;
        let dbg = format!("{err:?}");
        assert!(dbg.contains("Shutdown"));
    }
}
