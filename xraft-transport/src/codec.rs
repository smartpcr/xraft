use xraft_core::error::{Result, XraftError};
use xraft_core::rpc::RpcEnvelope;
use xraft_core::types::ClusterId;

/// Length-prefixed bincode codec for `RpcEnvelope`.
///
/// Wire format: `[payload_len: u32 big-endian][bincode bytes]`
///
/// The codec validates `cluster_id` on decode — envelopes whose
/// `cluster_id` doesn't match the expected value are rejected with
/// `XraftError::InvalidClusterId` before reaching the protocol layer.
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

    /// Serialize an `RpcEnvelope` into a length-prefixed byte buffer.
    pub fn encode(&self, envelope: &RpcEnvelope) -> Result<Vec<u8>> {
        let payload = bincode::serialize(envelope).map_err(|e| {
            XraftError::TransportError(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        let len = u32::try_from(payload.len()).map_err(|_| {
            XraftError::TransportError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "envelope too large for u32 length prefix",
            ))
        })?;
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// Deserialize a length-prefixed buffer into an `RpcEnvelope`.
    ///
    /// Rejects envelopes whose `cluster_id` doesn't match.
    /// Rejects frames with trailing bytes after the declared payload length.
    pub fn decode(&self, buf: &[u8]) -> Result<RpcEnvelope> {
        if buf.len() < 4 {
            return Err(XraftError::TransportError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "buffer too short for length prefix",
            )));
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let payload = &buf[4..];
        if payload.len() < len {
            return Err(XraftError::TransportError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "buffer shorter than declared length",
            )));
        }
        if payload.len() > len {
            return Err(XraftError::TransportError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "trailing bytes after declared payload length",
            )));
        }
        let envelope: RpcEnvelope =
            bincode::deserialize(payload).map_err(|e| {
                XraftError::TransportError(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
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
            leader_epoch: Term(1),
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

    #[test]
    fn round_trip() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let env = sample_envelope(cid);
        let encoded = codec.encode(&env).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn cluster_id_mismatch_rejected() {
        let cid_a = ClusterId(uuid::Uuid::new_v4());
        let cid_b = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid_b);
        let env = sample_envelope(cid_a);
        // Encode with a "wrong-cluster" codec that doesn't validate on encode
        let codec_a = RpcCodec::new(cid_a);
        let encoded = codec_a.encode(&env).unwrap();
        let result = codec.decode(&encoded);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, XraftError::InvalidClusterId),
            "expected InvalidClusterId, got {err:?}"
        );
    }

    #[test]
    fn truncated_buffer_rejected() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let env = sample_envelope(cid);
        let encoded = codec.encode(&env).unwrap();
        // Chop the buffer short
        let result = codec.decode(&encoded[..encoded.len() - 5]);
        assert!(result.is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let env = sample_envelope(cid);
        let mut encoded = codec.encode(&env).unwrap();
        // Append trailing garbage
        encoded.push(0xFF);
        encoded.push(0xAB);
        let result = codec.decode(&encoded);
        assert!(result.is_err(), "decode must reject frames with trailing bytes");
    }

    #[test]
    fn empty_buffer_rejected() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let codec = RpcCodec::new(cid);
        let result = codec.decode(&[]);
        assert!(result.is_err());
    }
}
