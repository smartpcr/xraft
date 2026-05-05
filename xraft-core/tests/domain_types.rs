use bytes::Bytes;
use std::net::SocketAddr;
use xraft_core::*;

#[test]
fn term_ordering() {
    assert!(Term(3) < Term(5));
    assert!(Term(5) > Term(3));
    assert_eq!(Term(3), Term(3));
    assert_ne!(Term(3), Term(5));
}

#[test]
fn log_entry_command_roundtrip() {
    let app_record = AppRecord {
        data: Bytes::from(vec![1u8, 2, 3, 4, 5]),
    };
    let payload = bincode::serialize(&app_record).unwrap();

    let entry = LogEntry {
        offset: Offset(42),
        term: Term(7),
        entry_type: EntryType::Command,
        payload: Bytes::from(payload),
    };

    let encoded = bincode::serialize(&entry).unwrap();
    let decoded: LogEntry = bincode::deserialize(&encoded).unwrap();

    assert_eq!(entry, decoded);

    // Verify the AppRecord inside can be recovered
    let recovered: AppRecord = bincode::deserialize(&decoded.payload).unwrap();
    assert_eq!(app_record, recovered);
}

#[test]
fn app_record_roundtrip_256_bytes() {
    let data: Vec<u8> = (0u8..=255).collect();
    assert_eq!(data.len(), 256);

    let record = AppRecord {
        data: Bytes::from(data.clone()),
    };

    let encoded = bincode::serialize(&record).unwrap();
    let decoded: AppRecord = bincode::deserialize(&encoded).unwrap();

    assert_eq!(record, decoded);
    assert_eq!(decoded.data.as_ref(), data.as_slice());
}

#[test]
fn app_snapshot_roundtrip() {
    let snapshot = AppSnapshot {
        data: vec![10, 20, 30, 40, 50],
    };

    let encoded = bincode::serialize(&snapshot).unwrap();
    let decoded: AppSnapshot = bincode::deserialize(&encoded).unwrap();

    assert_eq!(snapshot, decoded);
}

#[test]
fn quorum_state_roundtrip() {
    let state = QuorumState {
        current_term: Term(5),
        voted_for: Some(NodeId(3)),
        leader_id: Some(NodeId(1)),
        leader_epoch: Term(4),
    };

    let encoded = bincode::serialize(&state).unwrap();
    let decoded: QuorumState = bincode::deserialize(&encoded).unwrap();

    assert_eq!(state, decoded);
}

#[test]
fn voter_info_roundtrip() {
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let info = VoterInfo {
        node_id: NodeId(1),
        endpoint: addr,
    };

    let encoded = bincode::serialize(&info).unwrap();
    let decoded: VoterInfo = bincode::deserialize(&encoded).unwrap();

    assert_eq!(info, decoded);
}

#[test]
fn voters_record_roundtrip() {
    let record = VotersRecord {
        version: 1,
        voters: vec![
            VoterInfo {
                node_id: NodeId(1),
                endpoint: "10.0.0.1:9000".parse().unwrap(),
            },
            VoterInfo {
                node_id: NodeId(2),
                endpoint: "10.0.0.2:9000".parse().unwrap(),
            },
            VoterInfo {
                node_id: NodeId(3),
                endpoint: "10.0.0.3:9000".parse().unwrap(),
            },
        ],
    };

    let encoded = bincode::serialize(&record).unwrap();
    let decoded: VotersRecord = bincode::deserialize(&encoded).unwrap();

    assert_eq!(record, decoded);
}

#[test]
fn consensus_state_roundtrip() {
    let state = ConsensusState {
        node_id: NodeId(1),
        current_term: Term(10),
        role: Role::Leader,
        leader_id: Some(NodeId(1)),
        high_watermark: 500,
        log_end_offset: 510,
        voter_set: vec![VoterInfo {
            node_id: NodeId(1),
            endpoint: "127.0.0.1:5000".parse().unwrap(),
        }],
    };

    let encoded = bincode::serialize(&state).unwrap();
    let decoded: ConsensusState = bincode::deserialize(&encoded).unwrap();

    assert_eq!(state, decoded);
}

#[test]
fn snapshot_metadata_roundtrip() {
    let meta = SnapshotMetadata {
        last_included_offset: 100,
        last_included_term: Term(5),
        voters: vec![VoterInfo {
            node_id: NodeId(1),
            endpoint: "10.0.0.1:9000".parse().unwrap(),
        }],
        leader_epoch: Term(5),
    };

    let encoded = bincode::serialize(&meta).unwrap();
    let decoded: SnapshotMetadata = bincode::deserialize(&encoded).unwrap();

    assert_eq!(meta, decoded);
}

#[test]
fn full_snapshot_roundtrip() {
    let snap = Snapshot {
        metadata: SnapshotMetadata {
            last_included_offset: 200,
            last_included_term: Term(8),
            voters: vec![
                VoterInfo {
                    node_id: NodeId(1),
                    endpoint: "10.0.0.1:9000".parse().unwrap(),
                },
                VoterInfo {
                    node_id: NodeId(2),
                    endpoint: "10.0.0.2:9000".parse().unwrap(),
                },
            ],
            leader_epoch: Term(7),
        },
        app_snapshot: AppSnapshot {
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        },
    };

    let encoded = bincode::serialize(&snap).unwrap();
    let decoded: Snapshot = bincode::deserialize(&encoded).unwrap();

    assert_eq!(snap, decoded);
}

#[test]
fn snapshot_reader_chunks() {
    let data = Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let mut reader = SnapshotReader::new(data);

    assert_eq!(reader.len(), 10);
    assert!(!reader.is_empty());

    let (chunk1, done1) = reader.read_chunk(4);
    assert_eq!(chunk1.as_ref(), &[1, 2, 3, 4]);
    assert!(!done1);

    let (chunk2, done2) = reader.read_chunk(4);
    assert_eq!(chunk2.as_ref(), &[5, 6, 7, 8]);
    assert!(!done2);

    let (chunk3, done3) = reader.read_chunk(4);
    assert_eq!(chunk3.as_ref(), &[9, 10]);
    assert!(done3);
}

#[test]
fn snapshot_writer_accumulates() {
    let mut writer = SnapshotWriter::new();
    assert_eq!(writer.bytes_written(), 0);

    writer.write_chunk(&[1, 2, 3]);
    assert_eq!(writer.bytes_written(), 3);

    writer.write_chunk(&[4, 5]);
    assert_eq!(writer.bytes_written(), 5);

    let result = writer.finalize();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[test]
fn role_variants() {
    let roles = [
        Role::Unattached,
        Role::Follower,
        Role::Candidate,
        Role::Leader,
    ];
    for role in &roles {
        let encoded = bincode::serialize(role).unwrap();
        let decoded: Role = bincode::deserialize(&encoded).unwrap();
        assert_eq!(*role, decoded);
    }
}

#[test]
fn entry_type_variants() {
    let types = [
        EntryType::Command,
        EntryType::LeaderChangeMessage,
        EntryType::VotersRecord,
    ];
    for et in &types {
        let encoded = bincode::serialize(et).unwrap();
        let decoded: EntryType = bincode::deserialize(&encoded).unwrap();
        assert_eq!(*et, decoded);
    }
}
