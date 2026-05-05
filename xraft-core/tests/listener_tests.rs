use xraft_core::app_record::{AppRecord, AppSnapshot};
use xraft_core::listener::Listener;
use xraft_core::listener_event::ListenerEvent;
use xraft_core::snapshot::{
    Snapshot, SnapshotMetadata, SnapshotReader, VoterInfo,
};
use xraft_core::types::{NodeId, Term};

/// Mock listener that records all callbacks for test assertions.
#[derive(Debug, Default)]
struct MockListener {
    committed_batches: Vec<Vec<(u64, Vec<u8>)>>,
    snapshots_loaded: Vec<Vec<u8>>,
    leader_changes: Vec<(NodeId, Term)>,
    shutdown_called: bool,
}

impl Listener for MockListener {
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]) {
        let recorded: Vec<(u64, Vec<u8>)> = batch
            .iter()
            .map(|(offset, record)| (*offset, record.data.to_vec()))
            .collect();
        self.committed_batches.push(recorded);
    }

    fn handle_load_snapshot(&mut self, reader: SnapshotReader) {
        self.snapshots_loaded.push(reader.app_data().to_vec());
    }

    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term) {
        self.leader_changes.push((leader_id, term));
    }

    fn begin_shutdown(&mut self) {
        self.shutdown_called = true;
    }
}

fn make_snapshot_reader(data: &[u8]) -> SnapshotReader {
    SnapshotReader::new(Snapshot {
        metadata: SnapshotMetadata {
            last_included_offset: 10,
            last_included_term: 2,
            voters: vec![VoterInfo {
                node_id: NodeId(1),
                endpoint: "127.0.0.1:9000".to_string(),
            }],
            leader_epoch: 1,
        },
        app_snapshot: AppSnapshot::new(data.to_vec()),
    })
}

// ── Scenario: ListenerEvent coverage ──────────────────────────────────────
// Given each ListenerEvent variant, When pattern-matched, Then all variants
// are covered exhaustively.

#[test]
fn listener_event_exhaustive_match() {
    let events: Vec<ListenerEvent> = vec![
        ListenerEvent::Commit {
            batch: vec![(1, AppRecord::new(b"a".as_slice()))],
        },
        ListenerEvent::LoadSnapshot {
            reader: make_snapshot_reader(b"snap"),
        },
        ListenerEvent::LeaderChange {
            leader_id: NodeId(42),
            term: Term(5),
        },
        ListenerEvent::Shutdown,
    ];

    let mut variants_seen = [false; 4];
    for event in events {
        match event {
            ListenerEvent::Commit { .. } => variants_seen[0] = true,
            ListenerEvent::LoadSnapshot { .. } => variants_seen[1] = true,
            ListenerEvent::LeaderChange { .. } => variants_seen[2] = true,
            ListenerEvent::Shutdown => variants_seen[3] = true,
        }
    }

    assert!(
        variants_seen.iter().all(|&seen| seen),
        "Not all ListenerEvent variants were covered"
    );
}

// ── Scenario: Listener mock ───────────────────────────────────────────────
// Given a mock Listener impl, When handle_commit is called with a batch of
// (offset, AppRecord) pairs, Then the mock records the batch and can be
// asserted against.

#[test]
fn listener_mock_handle_commit() {
    let mut mock = MockListener::default();

    let batch = vec![
        (1, AppRecord::new(b"cmd-1".as_slice())),
        (2, AppRecord::new(b"cmd-2".as_slice())),
        (3, AppRecord::new(b"cmd-3".as_slice())),
    ];

    mock.handle_commit(&batch);

    assert_eq!(mock.committed_batches.len(), 1);
    let recorded = &mock.committed_batches[0];
    assert_eq!(recorded.len(), 3);
    assert_eq!(recorded[0], (1, b"cmd-1".to_vec()));
    assert_eq!(recorded[1], (2, b"cmd-2".to_vec()));
    assert_eq!(recorded[2], (3, b"cmd-3".to_vec()));
}

// ── Dispatch tests: ListenerEvent → Listener ─────────────────────────────

#[test]
fn dispatch_commit_event_invokes_handle_commit() {
    let mut mock = MockListener::default();

    let event = ListenerEvent::Commit {
        batch: vec![
            (10, AppRecord::new(b"alpha".as_slice())),
            (11, AppRecord::new(b"beta".as_slice())),
        ],
    };

    event.dispatch(&mut mock);

    assert_eq!(mock.committed_batches.len(), 1);
    assert_eq!(mock.committed_batches[0].len(), 2);
    assert_eq!(mock.committed_batches[0][0], (10, b"alpha".to_vec()));
    assert_eq!(mock.committed_batches[0][1], (11, b"beta".to_vec()));
}

#[test]
fn dispatch_load_snapshot_event_invokes_handle_load_snapshot() {
    let mut mock = MockListener::default();

    let event = ListenerEvent::LoadSnapshot {
        reader: make_snapshot_reader(b"snapshot-data"),
    };

    event.dispatch(&mut mock);

    assert_eq!(mock.snapshots_loaded.len(), 1);
    assert_eq!(mock.snapshots_loaded[0], b"snapshot-data".to_vec());
}

#[test]
fn dispatch_leader_change_event_invokes_handle_leader_change() {
    let mut mock = MockListener::default();

    let event = ListenerEvent::LeaderChange {
        leader_id: NodeId(7),
        term: Term(99),
    };

    event.dispatch(&mut mock);

    assert_eq!(mock.leader_changes.len(), 1);
    assert_eq!(mock.leader_changes[0], (NodeId(7), Term(99)));
}

#[test]
fn dispatch_shutdown_event_invokes_begin_shutdown() {
    let mut mock = MockListener::default();

    let event = ListenerEvent::Shutdown;

    event.dispatch(&mut mock);

    assert!(mock.shutdown_called);
}

#[test]
fn multiple_commits_recorded_separately() {
    let mut mock = MockListener::default();

    mock.handle_commit(&[(1, AppRecord::new(b"first".as_slice()))]);
    mock.handle_commit(&[(2, AppRecord::new(b"second".as_slice()))]);

    assert_eq!(mock.committed_batches.len(), 2);
    assert_eq!(mock.committed_batches[0][0], (1, b"first".to_vec()));
    assert_eq!(mock.committed_batches[1][0], (2, b"second".to_vec()));
}

#[test]
fn listener_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<MockListener>();
}

#[test]
fn listener_trait_requires_send() {
    fn accepts_listener<L: Listener>(_l: &L) {}
    let mock = MockListener::default();
    accepts_listener(&mock);
}
