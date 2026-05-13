//! Length-prefixed bincode codec for `RpcEnvelope`.
//!
//! Wire format: `[payload_len: u32 big-endian][bincode bytes]`
//!
//! The codec validates `cluster_id` on decode — envelopes whose
//! `cluster_id` doesn't match the expected value are rejected with
//! `XraftError::InvalidClusterId` before reaching the protocol layer.
//!
//! Intended for use by stream-oriented transports (e.g., the upcoming
//! TCP transport). The in-process `ChannelTransport` passes `RpcEnvelope`
//! values directly without going through the codec.

use xraft_core::error::XraftError;
use xraft_core::rpc::RpcEnvelope;
use xraft_core::types::ClusterId;

/// Maximum allowed frame payload size (64 MiB).
///
/// Protects against multi-GB allocations when composing this codec with a
/// streaming TCP reader — a corrupt or malicious length prefix cannot cause
/// the allocator to hand out more than this.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// Length-prefixed bincode codec for `RpcEnvelope`.
pub struct RpcCodec {
    expected_cluster_id: ClusterId,
}

impl RpcCodec {
    /// Create a codec that validates inbound envelopes against `cluster_id`.
    pub fn new(expected_cluster_id: ClusterId) -> Self {
        Self {
            expected_cluster_id,
        }
    }

    /// Cluster id this codec accepts on decode.
    pub fn expected_cluster_id(&self) -> ClusterId {
        self.expected_cluster_id
    }

    /// Serialize an `RpcEnvelope` into a length-prefixed byte buffer.
    pub fn encode(&self, envelope: &RpcEnvelope) -> Result<Vec<u8>, XraftError> {
        let payload = bincode::serialize(envelope).map_err(|e| XraftError::TransportError {
            reason: format!("bincode encode failed: {e}"),
        })?;
        let len = u32::try_from(payload.len()).map_err(|_| XraftError::TransportError {
            reason: "envelope too large for u32 length prefix".to_string(),
        })?;
        if len > MAX_FRAME_LEN {
            return Err(XraftError::TransportError {
                reason: format!(
                    "encoded envelope ({len} bytes) exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN} bytes)"
                ),
            });
        }
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// Deserialize a length-prefixed buffer into an `RpcEnvelope`.
    ///
    /// Rejects envelopes whose `cluster_id` doesn't match.
    /// Rejects frames with trailing bytes after the declared payload length.
    /// Rejects frames whose declared length exceeds `MAX_FRAME_LEN`.
    pub fn decode(&self, buf: &[u8]) -> Result<RpcEnvelope, XraftError> {
        if buf.len() < 4 {
            return Err(XraftError::TransportError {
                reason: "buffer too short for length prefix".to_string(),
            });
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if len > MAX_FRAME_LEN {
            return Err(XraftError::TransportError {
                reason: format!(
                    "declared frame length ({len} bytes) exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN} bytes)"
                ),
            });
        }
        let len = len as usize;
        let payload = &buf[4..];
        if payload.len() < len {
            return Err(XraftError::TransportError {
                reason: "buffer shorter than declared length".to_string(),
            });
        }
        if payload.len() > len {
            return Err(XraftError::TransportError {
                reason: "trailing bytes after declared payload length".to_string(),
            });
        }
        let envelope: RpcEnvelope =
            bincode::deserialize(payload).map_err(|e| XraftError::TransportError {
                reason: format!("bincode decode failed: {e}"),
            })?;
        if envelope.cluster_id != self.expected_cluster_id {
            return Err(XraftError::InvalidClusterId);
        }
        Ok(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::rpc::{RpcPayload, VoteRequest};
    use xraft_core::types::{NodeId, Term};

    fn sample_envelope(cluster_id: ClusterId) -> RpcEnvelope {
        RpcEnvelope {
            cluster_id,
            leader_epoch: 1,
            source: NodeId(1),
            payload: RpcPayload::VoteRequest(VoteRequest {
                term: Term(2),
                candidate_id: NodeId(1),
                last_log_offset: 0,
                last_log_term: Term(0),
                is_pre_vote: false,
            }),
        }
    }

    fn assert_envelopes_equal(a: &RpcEnvelope, b: &RpcEnvelope) {
        assert_eq!(a.cluster_id, b.cluster_id);
        assert_eq!(a.leader_epoch, b.leader_epoch);
        assert_eq!(a.source, b.source);
        match (&a.payload, &b.payload) {
            (RpcPayload::VoteRequest(x), RpcPayload::VoteRequest(y)) => {
                assert_eq!(x.term, y.term);
                assert_eq!(x.candidate_id, y.candidate_id);
                assert_eq!(x.last_log_offset, y.last_log_offset);
                assert_eq!(x.last_log_term, y.last_log_term);
                assert_eq!(x.is_pre_vote, y.is_pre_vote);
            }
            _ => panic!("payload variants differ"),
        }
    }

    #[test]
    fn round_trip() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let env = sample_envelope(cid);
        let encoded = codec.encode(&env).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_envelopes_equal(&env, &decoded);
    }

    #[test]
    fn cluster_id_mismatch_rejected() {
        let cid_a = ClusterId(uuid::Uuid::new_v4());
        let cid_b = ClusterId(uuid::Uuid::new_v4());
        let codec_b = RpcCodec::new(cid_b);
        let codec_a = RpcCodec::new(cid_a);
        let env = sample_envelope(cid_a);
        let encoded = codec_a.encode(&env).unwrap();
        let result = codec_b.decode(&encoded);
        match result {
            Err(XraftError::InvalidClusterId) => {}
            other => panic!("expected InvalidClusterId, got {other:?}"),
        }
    }

    #[test]
    fn truncated_buffer_rejected() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let env = sample_envelope(cid);
        let encoded = codec.encode(&env).unwrap();
        let result = codec.decode(&encoded[..encoded.len() - 5]);
        assert!(result.is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let env = sample_envelope(cid);
        let mut encoded = codec.encode(&env).unwrap();
        encoded.push(0xFF);
        encoded.push(0xAB);
        let result = codec.decode(&encoded);
        assert!(
            result.is_err(),
            "decode must reject frames with trailing bytes"
        );
    }

    #[test]
    fn empty_buffer_rejected() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let result = codec.decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn short_length_prefix_rejected() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let result = codec.decode(&[0u8, 0u8, 0u8]);
        assert!(result.is_err());
    }

    #[test]
    fn oversized_frame_rejected_on_decode() {
        let len = MAX_FRAME_LEN + 1;
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&[0u8; 4]);
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let result = codec.decode(&buf);
        assert!(
            result.is_err(),
            "decode must reject frames exceeding MAX_FRAME_LEN"
        );
    }
}
