# Implementation Plan — raft protocol

> **Story:** `raft:RAFT-PROTOCOL`
>
> **Sibling documents:** [tech-spec.md](./tech-spec.md) ·
> [architecture.md](./architecture.md) ·
> [e2e-scenarios.md](./e2e-scenarios.md)
>
> This is a LIVEDOC — checkboxes track implementation progress.
> Operators tick boxes as work lands; the doc is the source of truth for
> "what's done".
>
> **Repository state:** `smartpcr/xraft` is greenfield — no Rust source
> code exists yet. All crate names, module paths, and trait signatures
> reference the proposed designs in the architecture and tech-spec
> documents.

---

## Phase 1: Project Scaffolding and Core Types

> **Goal:** Establish the Cargo workspace, crate boundaries, shared types,
> and CI pipeline so that all subsequent phases have a compilable skeleton
> to build on. No consensus logic yet.
>
> **Sequencing:** This phase has no dependencies. All stages within it
> are sequential (each stage assumes the prior crate exists).

### Stage 1.1: Cargo Workspace and Root Configuration

#### Implementation Steps
- [ ] Create root `Cargo.toml` with `[workspace]` members: `xraft-core`, `xraft-storage`, `xraft-transport`, `xraft-test`
- [ ] Create `xraft-core/Cargo.toml` with dependencies: `tokio`, `serde`, `bincode`, `bytes`, `tracing`
- [ ] Create `xraft-storage/Cargo.toml` with dependency on `xraft-core`
- [ ] Create `xraft-transport/Cargo.toml` with dependency on `xraft-core`
- [ ] Create `xraft-test/Cargo.toml` with dev-dependency on all three crates
- [ ] Add workspace-level settings: `edition = "2021"`, `resolver = "2"`, `[workspace.lints.clippy] all = "deny"`
- [ ] Create `.github/workflows/ci.yml` with `cargo build --workspace`, `cargo test --workspace`, and `cargo clippy --workspace -- -D warnings`
- [ ] Verify `cargo check --workspace` passes with empty `lib.rs` files in each crate

#### Test Scenarios
- [ ] Scenario: Workspace compiles — Given the root `Cargo.toml` and four crate stubs, When `cargo check --workspace` is run, Then it exits with code 0 and no errors
- [ ] Scenario: Clippy passes — Given the empty workspace, When `cargo clippy --workspace -- -D warnings` is run, Then it exits with code 0

### Stage 1.2: Core Domain Types in `xraft-core`

#### Implementation Steps
- [ ] Create `xraft-core/src/types.rs` defining `NodeId(u64)`, `Term(u64)`, `ClusterId(uuid::Uuid)`, `Offset(u64)` as newtypes with `Ord`, `Hash`, `Serialize`, `Deserialize`
- [ ] Create `xraft-core/src/app_record.rs` defining `AppRecord { data: Bytes }` (opaque application command payload) and `AppSnapshot { data: Vec<u8> }` (opaque application snapshot payload) as newtype wrappers with `Serialize`/`Deserialize` — these must exist before trait definitions in Stage 1.4 reference them
- [ ] Create `xraft-core/src/log_entry.rs` defining `LogEntry { offset, term, entry_type, payload }` and `EntryType { Command, LeaderChangeMessage, VotersRecord }` enums — `Command` entries wrap an `AppRecord`; control records (`LeaderChangeMessage`, `VotersRecord`) are never exposed to the application's `StateMachine`
- [ ] Create `xraft-core/src/voter.rs` defining `VoterInfo { node_id, endpoint }` and `VotersRecord { version, voters }` structs
- [ ] Create `xraft-core/src/consensus_state.rs` defining `Role { Unattached, Follower, Candidate, Leader }` enum and `ConsensusState` struct with all fields from architecture doc §3.1
- [ ] Create `xraft-core/src/quorum_state.rs` defining `QuorumState { current_term, voted_for, leader_id, leader_epoch }` struct
- [ ] Create `xraft-core/src/follower_progress.rs` defining `FollowerProgress { node_id, fetch_offset, last_fetch_timestamp, is_voter }` struct
- [ ] Create `xraft-core/src/snapshot.rs` defining `SnapshotMetadata { last_included_offset, last_included_term, voters: Vec<VoterInfo>, leader_epoch }` and `Snapshot { metadata: SnapshotMetadata, app_snapshot: AppSnapshot }` composite struct (consensus metadata + application payload split per architecture §3.2); define `SnapshotReader` (wraps a snapshot chunk stream for follower restore) and `SnapshotWriter` (wraps a chunked write session for receiving snapshots from leader) — these types are referenced by `SnapshotIO` and `Listener` traits in Stage 1.4/1.6
- [ ] Create `xraft-core/src/lib.rs` re-exporting all type modules including `AppRecord`, `AppSnapshot`, `Snapshot`, `SnapshotMetadata`, `SnapshotReader`, and `SnapshotWriter`
- [ ] Add `uuid` and `bytes` crate dependencies to `xraft-core/Cargo.toml`

#### Test Scenarios
- [ ] Scenario: Type roundtrip serialisation — Given a `LogEntry` with `EntryType::Command` wrapping an `AppRecord`, When serialised with bincode and deserialised, Then the result equals the original including the `AppRecord` payload
- [ ] Scenario: Term ordering — Given `Term(3)` and `Term(5)`, When compared, Then `Term(3) < Term(5)` holds
- [ ] Scenario: AppRecord roundtrip — Given an `AppRecord` with a 256-byte command payload, When serialised with bincode and deserialised, Then the result equals the original byte-for-byte

### Stage 1.3: RPC Message Types

#### Implementation Steps
- [ ] Create `xraft-core/src/rpc.rs` defining `RpcEnvelope { cluster_id, leader_epoch, source, payload }` and `RpcPayload` enum
- [ ] Define `VoteRequest { term, candidate_id, last_log_offset, last_log_term, is_pre_vote }` and `VoteResponse { term, vote_granted, is_pre_vote }` in `rpc.rs`
- [ ] Define `FetchRequest { replica_id, fetch_offset, last_fetched_epoch, max_bytes }` and `FetchResponse { leader_id, leader_epoch, high_watermark, log_start_offset, entries, diverging_epoch, snapshot_id }` in `rpc.rs`
- [ ] Define `DivergingEpoch { epoch, end_offset }` and `SnapshotId { end_offset, epoch }` helper structs in `rpc.rs`
- [ ] Define `FetchSnapshotRequest` / `FetchSnapshotResponse` in `rpc.rs`
- [ ] Define `AddVoterRequest`, `RemoveVoterRequest`, `UpdateVoterRequest`, `MembershipChangeResponse`, and `MembershipError` enum in `rpc.rs`

#### Test Scenarios
- [ ] Scenario: RPC envelope serialisation — Given an `RpcEnvelope` wrapping a `VoteRequest`, When serialised to bincode and deserialised, Then all fields match the original
- [ ] Scenario: MembershipError variants — Given each `MembershipError` variant (`NotLeader`, `ChangeInProgress`, `NodeAlreadyVoter`, `NodeNotFound`, `NodeNotCaughtUp`), When pattern-matched, Then all five variants are covered exhaustively

### Stage 1.4: Trait Definitions (Storage, Transport, Runtime)

#### Implementation Steps
- [ ] Create `xraft-core/src/traits.rs` defining `#[async_trait] trait LogStore { async fn append(&mut self, entries: &[LogEntry]) -> Result<()>, async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>, async fn truncate_suffix(&mut self, from_offset: u64) -> Result<()>, async fn truncate_prefix(&mut self, up_to_offset: u64) -> Result<()>, fn log_start_offset(&self) -> u64, fn log_end_offset(&self) -> u64, async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>> }` — matches architecture §4.1 trait definition
- [ ] Define `#[async_trait] trait QuorumStateStore { async fn load(&self) -> Result<Option<QuorumState>>, async fn save(&self, state: &QuorumState) -> Result<()> }` in `traits.rs` — matches architecture §4.1
- [ ] Define `#[async_trait] trait SnapshotIO { async fn save(&self, snapshot: &Snapshot) -> Result<()>, async fn load_latest(&self) -> Result<Option<Snapshot>>, async fn read_chunk(&self, id: &SnapshotId, position: u64, max_bytes: u32) -> Result<(Bytes, bool)>, async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter> }` in `traits.rs` — matches architecture §4.1 method names exactly; uses `Snapshot`, `SnapshotId`, `SnapshotWriter` from Stage 1.2
- [ ] Define `#[async_trait] trait TransportSender: Send + Sync + 'static { async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()> }` in `traits.rs` — takes `&self` (shared reference) because `IoStage` may send to multiple peers concurrently; matches architecture §4.4 split design
- [ ] Define `#[async_trait] trait TransportReceiver: Send + 'static { async fn recv(&mut self) -> Result<RpcEnvelope> }` in `traits.rs` — takes `&mut self` (exclusive access) because only the `ReceiverTask` reads from the network; matches architecture §4.4 split design
- [ ] Define `#[async_trait] trait Clock: Send + 'static { fn now(&self) -> Instant; async fn sleep_until(&self, deadline: Instant); fn random_election_timeout(&self) -> Duration; }` in `traits.rs` — **Runtime** trait (not I/O), used directly by the `EventLoop` for timer management (election timeouts, check-quorum deadlines), NOT mediated by `IoAction` or the `IoStage` (architecture §4.1, §4.4); requires `#[async_trait]` because `sleep_until` is async and the trait is used as `Box<dyn Clock>` (trait object) — without `#[async_trait]`, native `async fn` in traits is not object-safe for `dyn Trait` dispatch; injected into `RaftNode` as `Box<dyn Clock>` and passed to the `EventLoop`, not the `IoStage`; does not require `Sync` because only the single-threaded event loop calls it; matches architecture §4.1 trait definition exactly (including `#[async_trait]` attribute and `Send + 'static` bounds)
- [ ] Define `trait StateMachine: Send + 'static { fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>; fn snapshot(&self) -> Result<AppSnapshot>; fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>; }` in `traits.rs` — synchronous trait (not `#[async_trait]`) per architecture §4.1; application callbacks are invoked synchronously by the `EventLoop`, not by the `IoStage`; uses `AppRecord` and `AppSnapshot` types from Stage 1.2
- [ ] Add `async-trait` dependency to `xraft-core/Cargo.toml`

#### Test Scenarios
- [ ] Scenario: Trait object safety — Given the `LogStore` trait, When a `Box<dyn LogStore>` is constructed from a mock, Then it compiles and can be called
- [ ] Scenario: Clock object safety — Given the `#[async_trait] Clock` trait with `async fn sleep_until`, When a `Box<dyn Clock>` is constructed from `SimulatedClock`, Then it compiles, is object-safe, and `sleep_until` can be called via dynamic dispatch
- [ ] Scenario: StateMachine trait contract — Given a dummy `StateMachine` impl, When `apply` is called with an `AppRecord` (from Stage 1.2), Then it returns `Ok(())`

### Stage 1.5: Error Types and Configuration

#### Implementation Steps
- [ ] Create `xraft-core/src/error.rs` defining `XraftError` enum with variants: `StorageError`, `TransportError`, `NotLeader`, `ProposalQueueFull`, `InvalidClusterId`, `Shutdown`
- [ ] Implement `std::error::Error` and `Display` for `XraftError`
- [ ] Create `xraft-core/src/config.rs` defining `RaftConfig { election_timeout_min_ms, election_timeout_max_ms, fetch_interval_ms, max_batch_size, max_fetch_bytes, snapshot_interval, data_dir }` with `Default` impl
- [ ] Add config validation: `election_timeout_min < election_timeout_max`, `fetch_interval < election_timeout_min`

#### Test Scenarios
- [ ] Scenario: Config validation — Given a `RaftConfig` where `election_timeout_min > election_timeout_max`, When validated, Then an error is returned
- [ ] Scenario: Default config valid — Given `RaftConfig::default()`, When validated, Then it passes and satisfies the Raft timing invariant constraints

### Stage 1.6: Listener Trait and ListenerEvent

> **Note:** `AppRecord` and `AppSnapshot` types are already created in Stage 1.2.
> This stage defines the application callback interface that uses those types.

#### Implementation Steps
- [ ] Create `xraft-core/src/listener.rs` defining `trait Listener: Send + 'static` with callbacks: `fn handle_commit(&mut self, batch: &[(u64, AppRecord)])`, `fn handle_load_snapshot(&mut self, reader: SnapshotReader)`, `fn handle_leader_change(&mut self, leader_id: NodeId, term: Term)`, `fn begin_shutdown(&mut self)` — synchronous trait (not `#[async_trait]`) modelled on KRaft's `RaftClient.Listener`; invoked by the `EventLoop` during message processing, before any `IoAction` is dispatched; uses `AppRecord` from Stage 1.2 and `SnapshotReader` from Stage 1.2; matches architecture §4.1 trait signature exactly
- [ ] Create `xraft-core/src/listener_event.rs` defining `ListenerEvent` enum with variants matching each `Listener` callback (`Commit`, `LoadSnapshot`, `LeaderChange`, `Shutdown`), used by the `EventLoop` for internal callback dispatch — these are synchronous in-process calls, NOT `IoAction` variants (architecture §3.2 note)
- [ ] Update `xraft-core/src/lib.rs` to re-export `Listener` and `ListenerEvent`

#### Test Scenarios
- [ ] Scenario: ListenerEvent coverage — Given each `ListenerEvent` variant, When pattern-matched, Then all variants are covered exhaustively
- [ ] Scenario: Listener mock — Given a mock `Listener` impl, When `handle_commit` is called with a batch of `(offset, AppRecord)` pairs, Then the mock records the batch and can be asserted against

### Stage 1.7: `RaftNode` Public API Skeleton

#### Implementation Steps
- [ ] Create `xraft-core/src/raft_node.rs` defining the `RaftNode<S: StateMachine, L: Listener>` struct with fields: `config: RaftConfig`, `event_loop_handle`, `propose_tx: mpsc::Sender` — this is the public entry point from architecture §2.1; generic over both application-provided types `S` and `L` (monomorphised at compile time per architecture §4.1); I/O traits (`LogStore`, `TransportSender`, `TransportReceiver`, `QuorumStateStore`, `SnapshotIO`) are injected as `Box<dyn ...>` trait objects at construction time — `TransportSender` is passed to the `IoStage` for outbound RPCs, `TransportReceiver` is passed to the `ReceiverTask` for inbound RPCs (per architecture §4.4); the `Clock` **Runtime** trait is also injected as `Box<dyn Clock>` but is passed to the `EventLoop` for timer management, not the `IoStage` (architecture §4.1 — Clock is not mediated by `IoAction`)
- [ ] Define `RaftNode::new(config, log_store, quorum_state_store, snapshot_io, transport_sender, transport_receiver, clock, state_machine, listener) -> Result<Self>` constructor that accepts I/O trait objects (separate `Box<dyn TransportSender>` and `Box<dyn TransportReceiver>` — callers use `TcpTransport::split()` or `ChannelTransport::split()` to obtain the halves per architecture §4.4), the `Clock` Runtime trait object (`Box<dyn Clock>` — passed to the `EventLoop`, not the `IoStage`), and application-provided `S` / `L` instances; initialises struct fields and event-loop channel; does not start the event loop (started in Phase 4) or run recovery (completed in Phase 6)
- [ ] Define `RaftNode::propose(command: AppRecord) -> Future<Result<Offset>>` method that sends the command to the event-loop channel and returns a future; returns `NotLeader` when no leader is active (consensus path completed in Phase 5)
- [ ] Define `RaftNode::read() -> Result<ConsensusState>` method returning the current committed protocol state (current term, leader, high_watermark, role, voter set) — per tech-spec §2.1.5 the initial implementation routes reads through the log for safety, meaning the returned state reflects the latest HW-committed position in the log; the high_watermark in the returned state is an exclusive upper bound (entry at offset O is committed when O < HW, per architecture §3.1); linearisable reads (read-index, lease-based) are out of scope per tech-spec §2.2
- [ ] Define `RaftNode::bootstrap(cluster_id: ClusterId, initial_voters: Vec<VoterInfo>) -> Result<()>` method that validates preconditions (empty log, no quorum-state file, no existing snapshot) and stores configuration; bootstrap logic completed in Phase 6 — `ClusterId` is provided by the caller, not generated internally, to ensure all nodes share the same cluster identity
- [ ] Define `RaftNode::shutdown() -> Result<()>` lifecycle method that invokes `Listener::begin_shutdown()` synchronously within the event loop before draining pending I/O and stopping the event loop
- [ ] Update `xraft-core/src/lib.rs` to re-export `RaftNode`

#### Test Scenarios
- [ ] Scenario: RaftNode compiles — Given the `RaftNode<S: StateMachine, L: Listener>` struct with all type parameters, When `cargo check` is run, Then it compiles without errors
- [ ] Scenario: API surface exists — Given a `RaftNode` instance constructed with mock `StateMachine`, mock `Listener`, and mock I/O trait objects, When `propose()`, `read()`, `bootstrap()`, and `shutdown()` are called, Then each returns a typed `Result` with defined error variants

### Stage 1.8: In-Memory Storage Backends and Simulated Clock (`xraft-test`)

> **Note:** These test-only implementations depend only on the traits and types
> defined in Stages 1.2–1.4. They are placed here (rather than in a later
> phase) because Phases 4–8 use them as test infrastructure for deterministic
> scenario testing. The `xraft-test` crate is the canonical location for all
> test-only code — no test utilities live in `xraft-core`.

#### Implementation Steps
- [ ] Create `xraft-test/src/memory_log.rs` implementing `LogStore` trait with `Vec<LogEntry>` backend — supports append, read `[start_offset, end_offset)`, truncate_suffix, truncate_prefix, offset tracking
- [ ] Implement optional fault injection in `MemoryLogStore`: configurable `fsync` failure probability, write corruption injection
- [ ] Create `xraft-test/src/memory_snapshot.rs` implementing `SnapshotIO` trait with in-memory snapshot storage — method names match architecture §4.1: `save`, `load_latest`, `read_chunk`, `begin_receive`
- [ ] Create `xraft-test/src/memory_quorum_state.rs` implementing `QuorumStateStore` trait with in-memory state — `load()` returns `None` when no state has been saved (matching architecture §4.1 `Option<QuorumState>` contract)
- [ ] Create `xraft-test/src/simulated_clock.rs` implementing `SimulatedClock` — deterministic clock implementing the `#[async_trait] Clock` **Runtime** trait from `xraft-core` (methods: `now`, `sleep_until`, `random_election_timeout` matching architecture §4.1; `Clock` requires `#[async_trait]` for `Box<dyn Clock>` object safety; used by the `EventLoop` for timer management, not by the `IoStage`), advances only when explicitly ticked via `advance(duration)`, allows precise control over election timeouts, fetch intervals, and Check Quorum deadlines

#### Test Scenarios
- [ ] Scenario: Memory log store — Given a `MemoryLogStore`, When 1000 entries are appended and read back, Then all entries match with correct offsets
- [ ] Scenario: Fault injection — Given a `MemoryLogStore` with fsync failure probability 1.0, When `append` is called, Then it returns a `StorageError`
- [ ] Scenario: Simulated clock — Given a `SimulatedClock` at time 0, When advanced by 150 ms, Then `now()` returns 150 ms and no wall-clock time has passed

---

## Phase 2: Storage Layer (`xraft-storage`)

> **Goal:** Implement the durable storage backend: segmented log, quorum-state
> file, leader-epoch checkpoint, and snapshot store. All operations fsync
> before returning. Depends on Phase 1 completing.
>
> **Sequencing:** Stages 2.1–2.4 are parallel-safe (independent storage
> components). Stage 2.5 depends on all prior stages.

### Stage 2.1: Segment Log — Write Path

#### Implementation Steps
- [ ] Create `xraft-storage/src/segment.rs` implementing a single log segment: append entries, flush/fsync, and read back by offset range
- [ ] Implement CRC-32C checksum per batch written to a segment file (using `crc32c` crate)
- [ ] Create `xraft-storage/src/segment_index.rs` implementing sparse index (every Nth entry → byte position) for O(log n) offset lookups
- [ ] Create `xraft-storage/src/segment_log.rs` managing a series of segments: roll to new segment when size threshold is reached, map offset to correct segment file
- [ ] Implement `LogStore::append` — serialize entries with bincode, write batch with CRC, update sparse index, fsync
- [ ] Create data directory layout: `data/<cluster_id>/log/` with `.log` and `.index` files named by base offset (zero-padded 20 digits)

#### Test Scenarios
- [ ] Scenario: Append and read back — Given an empty segment log, When 100 entries are appended, Then `read(0, 100)` returns all 100 entries with matching offsets, terms, and payloads
- [ ] Scenario: CRC integrity — Given a segment file, When a byte is corrupted mid-segment, Then reading past the corruption point returns a `StorageError`
- [ ] Scenario: Segment rollover — Given a segment size limit of 1 KB, When entries exceeding 1 KB total are appended, Then a new segment file is created

### Stage 2.2: Segment Log — Read and Truncation

#### Implementation Steps
- [ ] Implement `LogStore::read(start_offset, end_offset)` — read entries in range `[start_offset, end_offset)` (exclusive end, matching architecture §4.1); locate segment via sparse index, seek to start, deserialize and validate CRC
- [ ] Implement `LogStore::entry_at(offset)` — read a single entry at the given offset, returning `None` if offset is outside the log bounds
- [ ] Implement `LogStore::truncate_suffix(offset)` — remove all entries at and after the given offset (for log divergence), truncate the segment file, update the index
- [ ] Implement `LogStore::truncate_prefix(offset)` — delete segment files whose entries are all before the given offset (for log compaction after snapshot), update `log_start_offset`
- [ ] Implement recovery scan on startup: walk segments forward, validate CRCs, truncate at first corruption

#### Test Scenarios
- [ ] Scenario: Truncate suffix — Given a log with entries 0–99, When `truncate_suffix(50)` is called, Then `read(0, 100)` returns only entries 0–49
- [ ] Scenario: Truncate prefix — Given a log with 3 segment files covering offsets 0–2999, When `truncate_prefix(1000)` is called, Then the first segment file is deleted and `log_start_offset()` returns 1000
- [ ] Scenario: Recovery after crash — Given a segment with a partially-written (corrupt CRC) final batch, When recovery scan runs, Then the corrupt batch is truncated and all entries before it are intact

### Stage 2.3: Quorum State File and Leader-Epoch Checkpoint

#### Implementation Steps
- [ ] Create `xraft-storage/src/quorum_state_file.rs` implementing `QuorumStateStore` — write JSON to temp file, fsync, atomic rename to `quorum-state`
- [ ] Implement `QuorumStateStore::load` — read and parse `quorum-state` file on startup; return `None` if file does not exist (per architecture §4.1 `Option<QuorumState>` return type — the recovery code in Phase 6 interprets `None` as initial state with term=0 and no vote)
- [ ] Create `xraft-storage/src/leader_epoch_checkpoint.rs` — persist and cache mapping of `leader_epoch → start_offset`
- [ ] Implement checkpoint append (new epoch entry) and lookup (find start offset for a given epoch) with binary search

#### Test Scenarios
- [ ] Scenario: Atomic quorum-state write — Given an existing `quorum-state` file, When a new state is persisted and the process crashes mid-write, Then the old file is still readable (atomic rename guarantees)
- [ ] Scenario: Leader epoch lookup — Given epochs {1→0, 3→50, 5→120}, When looking up epoch 3, Then `start_offset = 50` is returned
- [ ] Scenario: Missing quorum-state — Given no `quorum-state` file exists, When `load()` is called, Then `None` is returned (per architecture §4.1 `Option<QuorumState>` contract); the recovery code (Phase 6) interprets `None` as initial state with term=0 and no vote

### Stage 2.4: Snapshot Store

#### Implementation Steps
- [ ] Create `xraft-storage/src/snapshot_store.rs` implementing `SnapshotIO` — method names match architecture §4.1: `save`, `load_latest`, `read_chunk`, `begin_receive`; write snapshot atomically (temp file → fsync → rename) to `data/<cluster_id>/log/snapshot/<offset>-<term>.snap`
- [ ] Implement `load_latest` — scan snapshot directory, parse filenames, return the `Snapshot` with the highest `last_included_offset`
- [ ] Implement chunked read (`read_chunk(snapshot_id, position, max_bytes)`) for `FetchSnapshot` RPC serving
- [ ] Implement `begin_receive(snapshot_id) -> SnapshotWriter` for receiving snapshot from leader, assembling chunks, and atomic finalization

#### Test Scenarios
- [ ] Scenario: Save and load snapshot — Given a `Snapshot` with metadata and app payload, When saved and then loaded, Then all fields match
- [ ] Scenario: Chunked read — Given a 10 KB snapshot, When read in 1 KB chunks, Then 10 chunks are returned and concatenation equals the original bytes
- [ ] Scenario: Latest snapshot selection — Given snapshots at offsets 100, 500, and 300, When `load_latest` is called, Then the `Snapshot` at offset 500 is returned

### Stage 2.5: Storage Integration and `StorageEngine` Facade

#### Implementation Steps
- [ ] Create `xraft-storage/src/lib.rs` defining `StorageEngine` struct that owns `SegmentLog`, `QuorumStateFile`, `LeaderEpochCheckpoint`, and `SnapshotStore`
- [ ] Implement `StorageEngine::open(config)` — create directory layout, open or recover log, load quorum state, load latest snapshot, build leader-epoch checkpoint from log scan
- [ ] Expose `StorageEngine` fields via the individual trait objects (`LogStore`, `QuorumStateStore`, `SnapshotIO`)
- [ ] Ensure all public types implement `Send + Sync` for async usage

#### Test Scenarios
- [ ] Scenario: Full lifecycle — Given a fresh data directory, When `StorageEngine::open` is called then entries are appended, a snapshot is taken, prefix is truncated, and engine is re-opened, Then all data is recovered correctly
- [ ] Scenario: Concurrent trait usage — Given a `StorageEngine`, When `LogStore` and `QuorumStateStore` are used from different async tasks, Then no deadlocks or data races occur (compiles with `Send + Sync` bounds)

---

## Phase 3: Transport Layer (`xraft-transport`)

> **Goal:** Implement the async RPC transport — both a production TCP
> transport and an in-process channel transport for testing. Depends on
> Phase 1 types. Parallel-safe with Phase 2.
>
> **Sequencing:** Stages 3.1 and 3.2 are parallel-safe. Stage 3.3
> depends on both.

### Stage 3.1: RPC Codec and Channel Transport

#### Implementation Steps
- [ ] Create `xraft-transport/src/codec.rs` implementing `RpcCodec` — length-prefixed bincode serialisation/deserialisation of `RpcEnvelope`
- [ ] Create `xraft-transport/src/channel.rs` implementing `ChannelTransport` — in-process transport using `tokio::sync::mpsc` channels per node pair; exposes `split() -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>)` per architecture §4.4
- [ ] Implement `TransportSender::send` on `ChannelSender` — serialize envelope, route to destination channel (takes `&self` for concurrent sends)
- [ ] Implement `TransportReceiver::recv` on `ChannelReceiver` — await the next inbound `RpcEnvelope` from the node's inbound channel (takes `&mut self`, exclusive access per architecture §4.4)
- [ ] Implement cluster-id and leader-epoch fencing checks in the codec layer (reject envelopes with wrong cluster_id)

#### Test Scenarios
- [ ] Scenario: Send and receive — Given two nodes connected via `ChannelTransport`, When node A sends a `VoteRequest` to node B, Then node B receives it with all fields intact
- [ ] Scenario: Cluster ID fencing — Given node A sends an envelope with `cluster_id = X`, When node B expects `cluster_id = Y`, Then the message is rejected

### Stage 3.2: Network Simulator (Fault Injection)

#### Implementation Steps
- [ ] Create `xraft-transport/src/simulator.rs` implementing `NetworkSimulator` wrapping `ChannelTransport`
- [ ] Implement message drop with configurable probability per link
- [ ] Implement message delay with configurable latency range per link
- [ ] Implement message reordering (buffer and shuffle before delivery)
- [ ] Implement network partition (full and asymmetric) — block messages between specified node sets
- [ ] Implement `heal_partition()` to restore connectivity

#### Test Scenarios
- [ ] Scenario: Full partition — Given a 3-node cluster, When a partition isolates N1 from {N2, N3}, Then N1 receives no messages from N2 or N3 and vice versa
- [ ] Scenario: Asymmetric partition — Given a directed partition where N1→N2 is blocked but N2→N1 is allowed, When N1 sends to N2, Then the message is dropped, but when N2 sends to N1, Then the message is delivered
- [ ] Scenario: Message delay — Given a 200 ms delay on a link, When a message is sent, Then it is delivered after at least 200 ms

### Stage 3.3: TCP Transport (Production)

#### Implementation Steps
- [ ] Create `xraft-transport/src/tcp.rs` implementing `TcpTransport` using `tokio::net::TcpListener` and `TcpStream`; exposes `split() -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>)` per architecture §4.4
- [ ] Implement connection pooling: maintain one persistent connection per peer, reconnect on failure with exponential backoff
- [ ] Implement length-prefixed framing using `tokio_util::codec::LengthDelimitedCodec`
- [ ] Implement `TransportSender::send` on `TcpSender` — lookup or establish connection, write framed message (takes `&self`, requires `Send + Sync` for concurrent sends by `IoStage`)
- [ ] Implement `TransportReceiver::recv` on `TcpReceiver` — accept inbound connections, decode frames, return next `RpcEnvelope` (takes `&mut self`, exclusive access by `ReceiverTask` per architecture §4.4)
- [ ] Add `tokio-util` dependency to `xraft-transport/Cargo.toml`

#### Test Scenarios
- [ ] Scenario: TCP roundtrip — Given two `TcpTransport` instances bound to localhost ports, When node A sends a `FetchRequest` to node B, Then node B receives it correctly
- [ ] Scenario: Reconnection — Given node B's TCP listener is stopped and restarted, When node A sends a message, Then it reconnects and delivers the message after backoff

---

## Phase 4: Election and Leader Lifecycle (`xraft-core`)

> **Goal:** Implement leader election including Pre-Vote, term
> management, vote persistence, leader step-down, and Check Quorum.
> This is the first consensus logic. Depends on Phase 1 (types/traits
> including Stage 1.8 in-memory backends and SimulatedClock for
> deterministic test scenarios), Phase 2 (quorum state persistence),
> and Stage 3.1 (ChannelTransport for testing).
>
> **Sequencing:** Stages 4.1–4.2 are sequential. Stages 4.3–4.4 depend
> on 4.2.

### Stage 4.1: Event Loop Skeleton and Timer Infrastructure

#### Implementation Steps
- [ ] Create `xraft-core/src/event_loop.rs` with async event loop skeleton: inbound message channel (`tokio::sync::mpsc`), tick-based timer, and shutdown signal
- [ ] Implement `IoAction` enum: `PersistQuorumState`, `AppendLog`, `TruncateSuffix`, `TruncatePrefix`, `SendRpc`, `SaveSnapshot` — per architecture §3.2, application callbacks (`StateMachine::apply`, `Listener::handle_commit`, `Listener::handle_leader_change`) are NOT `IoAction` variants; they are synchronous, in-process calls invoked directly by the `EventLoop` during message processing, before the `IoAction` batch is produced
- [ ] Implement `IoActionBatch` collection and `IoStage` executor that dispatches actions to I/O trait objects (`LogStore`, `TransportSender`, `QuorumStateStore`, `SnapshotIO`) concurrently — the `IoStage` does NOT call `Clock` or `TransportReceiver` (architecture §3.2 note: Clock is used by the EventLoop, TransportReceiver by the ReceiverTask)
- [ ] Create `xraft-core/src/clock.rs` with `WallClock` (production implementation using `tokio::time`) of the `#[async_trait] Clock` trait — implements `now`, `sleep_until`, and `random_election_timeout` per architecture §4.1; `WallClock` implements `#[async_trait] Clock` for object-safe `Box<dyn Clock>` dispatch; `SimulatedClock` is defined in `xraft-test` (Stage 1.8), not in `xraft-core`, to keep test-only code out of the production crate
- [ ] Implement randomised election timeout generation using `Clock::random_election_timeout` with jitter in [min, max] range

#### Test Scenarios
- [ ] Scenario: Simulated clock — Given a `SimulatedClock` (from `xraft-test`, Stage 1.8) at time 0, When advanced by 150 ms, Then `now()` returns 150 ms and no wall-clock time has passed
- [ ] Scenario: IoStage dispatch — Given an `IoActionBatch` with a `PersistQuorumState` and a `SendRpc`, When executed by `IoStage`, Then both the quorum-state store and transport sender receive the respective calls; `Clock` and `TransportReceiver` are NOT invoked by the `IoStage`

### Stage 4.2: Election Manager — Vote Request/Response

#### Implementation Steps
- [ ] Create `xraft-core/src/election.rs` implementing `ElectionManager` struct
- [ ] Implement follower election timeout expiry → transition to Candidate, increment term, vote for self, broadcast `VoteRequest`
- [ ] Implement `handle_vote_request` — grant vote if: (a) requester term ≥ current term, (b) not already voted in this term or voted for same candidate, (c) requester log is at least as up-to-date (last log term/offset comparison)
- [ ] Implement `handle_vote_response` — collect votes; transition to Leader when a majority is reached
- [ ] Implement term update rule: any message with a higher term causes immediate step-down to Follower, clears `voted_for`
- [ ] Persist `voted_for` and `current_term` via `QuorumStateStore` before sending any vote response (fsync-before-ack)

#### Test Scenarios
- [ ] Scenario: Successful election — Given a 3-node cluster with simulated clock, When N1's election timeout expires and N2, N3 grant votes, Then N1 becomes Leader for the new term
- [ ] Scenario: Split vote — Given N1 and N2 both become candidates for the same term, When neither gets a majority, Then both return to Follower/Candidate and a new election starts with incremented term
- [ ] Scenario: Stale term rejection — Given N1 is at term 5, When N2 sends a VoteRequest for term 3, Then N1 rejects the vote
- [ ] Scenario: Log up-to-date check — Given N1 has log ending at (term=3, offset=10) and N2 at (term=3, offset=8), When N2 requests a vote, Then N1 rejects because N2's log is less up-to-date

### Stage 4.3: Pre-Vote Protocol

#### Implementation Steps
- [ ] Extend `ElectionManager` with Pre-Vote phase: before incrementing term, send `VoteRequest` with `is_pre_vote = true`
- [ ] Implement Pre-Vote response handling: only proceed to real election if a pre-vote majority is received
- [ ] Implement Pre-Vote rejection rule: follower rejects pre-vote if it has heard from a valid leader within the election timeout window (prevents disruptive elections from isolated nodes)
- [ ] Ensure Pre-Vote does not persist any state changes (term is not incremented, `voted_for` is not set)

#### Test Scenarios
- [ ] Scenario: Pre-Vote prevents disruptive election — Given N3 is partitioned from the leader N1, When N3's election timeout expires and it sends pre-votes, Then N2 (which recently heard from N1) rejects the pre-vote, preventing N3 from starting a real election
- [ ] Scenario: Pre-Vote success — Given no leader exists, When N1 sends pre-votes and gets majority, Then N1 proceeds to real election with term increment

### Stage 4.4: Check Quorum and Leader Step-Down

#### Implementation Steps
- [ ] Implement Check Quorum in the event loop: leader periodically (every `election_timeout` interval) checks if a majority of voters have sent a Fetch request within the timeout window
- [ ] If Check Quorum fails (leader cannot confirm majority), leader transitions to Follower state
- [ ] Implement leader step-down on receiving a message with a higher term
- [ ] Implement no-op `LeaderChangeMessage` append on leader election: new leader appends a control record to establish commit state for the new term; invoke `Listener::handle_leader_change(leader_id, term)` synchronously within the event loop to notify the application of the leadership change (architecture §4.1 `Listener` trait)

#### Test Scenarios
- [ ] Scenario: Check Quorum pass — Given leader N1 with recent fetches from N2 and N3 (majority of 3), When Check Quorum runs, Then N1 remains Leader
- [ ] Scenario: Check Quorum fail — Given leader N1 with no recent fetches from any follower, When Check Quorum deadline expires, Then N1 steps down to Follower
- [ ] Scenario: LeaderChangeMessage — Given N1 wins election for term 5, When it becomes Leader, Then a `LeaderChangeMessage` entry is appended to the log with term 5

---

## Phase 5: Pull-Based Log Replication (`xraft-core`)

> **Goal:** Implement the KRaft-style pull-based replication: followers
> periodically Fetch from leader, leader responds with entries and HW,
> high watermark advancement, divergence detection and log truncation.
> Depends on Phase 4 (election/leader lifecycle).
>
> **Sequencing:** Stages 5.1–5.2 are sequential. Stage 5.3 depends on
> 5.2.
>
> **HW semantics resolution (cross-document):** The tech-spec (§2.1.1,
> §8 Glossary) describes HW with inclusive phrasing: "entries ≤ HW are
> committed." The architecture (§3.1) formally defines HW as an
> **exclusive upper bound**: "entry at offset O is committed ⟺ O < HW."
> These are two notations for the same protocol state, not a protocol
> disagreement. The relationship is: tech-spec HW_inclusive + 1 =
> architecture HW_exclusive. For example, when the tech-spec says "HW
> advances to 5 and entries ≤ 5 are committed," the implementation
> stores HW = 6 and entries [0, 6) are committed — identical committed
> set. The exclusive notation is the natural fit because `fetch_offset`
> and `log_end_offset` are already exclusive (next-to-write), so the
> HW calculation (sort descending, take index ⌊V/2⌋) directly produces
> an exclusive value. **This plan and all implementation steps use
> exclusive HW semantics throughout.** No behavioural difference exists
> between the two notations; only the numeric convention differs. The
> e2e-scenarios document (§1 Offset Conventions) independently confirms
> this mapping.

### Stage 5.1: Fetch RPC — Follower Side

#### Implementation Steps
- [ ] Create `xraft-core/src/replication.rs` implementing `ReplicationManager` struct
- [ ] Implement follower periodic Fetch: on each `fetch_interval` tick, send `FetchRequest` to known leader with current `fetch_offset` and `last_fetched_epoch`
- [ ] Implement `handle_fetch_response` on follower: append received entries to local log, advance local `high_watermark` to `min(leader_HW, local_log_end_offset)` (architecture §5.10 rule 4); when HW advances, execute the three-phase commit notification in fixed order per architecture §4.1: (1) `StateMachine::apply` once per newly committed `Command` entry (skip control records — process `VotersRecord`/`LeaderChangeMessage` internally for bookkeeping), (2) `Listener::handle_commit` once with the batch of newly committed `AppRecord` values, (3) `DeferredCompletionQueue::complete` for all entries with offset < new HW; all three steps are synchronous in-process calls before any `IoAction` is produced; reset election timer
- [ ] Implement divergence handling: if response contains `DivergingEpoch`, truncate local log to `end_offset` and re-fetch from the truncation point
- [ ] Handle `snapshot_id` in Fetch response: if present, initiate snapshot transfer flow (delegate to SnapshotCoordinator, implemented in Phase 7)
- [ ] Handle leader-not-known state: if no leader is known, do not send Fetch (wait for election)

#### Test Scenarios
- [ ] Scenario: Normal replication — Given leader N1 with entries 0–9, When follower N2 sends Fetch(offset=0), Then N2 receives entries 0–9 and appends them locally
- [ ] Scenario: Incremental fetch — Given N2 has entries 0–4, When N2 sends Fetch(offset=5) and leader has entries 0–9, Then N2 receives entries 5–9 only
- [ ] Scenario: Election timer reset — Given follower N2, When it receives a Fetch response (even empty), Then its election timer is reset
- [ ] Scenario: Divergence truncation — Given N2 has entries diverging at offset 5, When Fetch response includes `DivergingEpoch{epoch=2, end_offset=5}`, Then N2 truncates its log to offset 5 and re-fetches

### Stage 5.2: Fetch RPC — Leader Side

#### Implementation Steps
- [ ] Implement `handle_fetch_request` on leader: validate cluster_id, validate fetch_offset against leader-epoch checkpoint
- [ ] Implement log divergence detection: compare follower's `last_fetched_epoch` against the leader-epoch checkpoint; if mismatch, compute `DivergingEpoch` and include in response
- [ ] Read entries from log starting at `fetch_offset` up to `max_bytes` limit and include in response
- [ ] Update `FollowerProgress.fetch_offset` and `last_fetch_timestamp` on each valid Fetch
- [ ] Include current `high_watermark` and `log_start_offset` in every Fetch response
- [ ] If `fetch_offset < log_start_offset` (follower is behind compacted entries), include `snapshot_id` in response to trigger snapshot transfer

#### Test Scenarios
- [ ] Scenario: Leader serves entries — Given leader N1 with entries 0–20, When follower sends Fetch(offset=10, max_bytes=4096), Then response contains entries starting at offset 10 up to the byte limit
- [ ] Scenario: Divergence detection — Given leader at epoch 5 starting at offset 50 and follower claims `last_fetched_epoch=3` at offset 60, When leader checks epoch checkpoint, Then it computes `DivergingEpoch` and sends truncation instruction
- [ ] Scenario: Follower progress tracking — Given N2 sends Fetch(offset=15), When leader processes it, Then `FollowerProgress` for N2 shows `fetch_offset=15`

### Stage 5.3: High Watermark Advancement and Commit Notification

#### Implementation Steps
- [ ] Implement high watermark calculation per architecture §5.2: after each incoming Fetch, leader collects `fetch_offset` for every voter (including itself — using its own `log_end_offset`), sorts them in **descending** order, and sets HW to the value at index `⌊V/2⌋` (0-indexed, where V is the total number of voters); this is the highest offset at or above which at least a majority (`⌊V/2⌋ + 1`) of voters have replicated; HW is an **exclusive upper bound** — entry at offset O is committed when `O < HW`; HW can only advance forward (never decreases)
- [ ] Create `xraft-core/src/batch_accumulator.rs` implementing `BatchAccumulator` — buffers incoming `propose()` calls, drains to log on tick or when batch is full
- [ ] Create `xraft-core/src/deferred_completion.rs` implementing `DeferredCompletionQueue` — parks `oneshot::Sender` futures keyed by offset, completes them when HW advances past their offset
- [ ] Wire HW advancement to the three-phase commit notification per architecture §4.1: when HW advances on the leader, execute in fixed order: (1) `StateMachine::apply` once per newly committed `Command` entry (control records processed internally), (2) `Listener::handle_commit` once with batch of newly committed `AppRecord` values, (3) `DeferredCompletionQueue::complete` for all entries with offset < new HW — all three steps are synchronous in-process calls within the event loop, NOT mediated by `IoAction`/`IoStage`
- [ ] Implement `propose()` public API on `RaftNode`: accept command, push to `BatchAccumulator`, return a `Future<Result<Offset>>` from `DeferredCompletionQueue`
- [ ] Implement `read()` public API on `RaftNode`: return the current committed protocol state as a `ConsensusState` snapshot (current term, role, leader_id, high_watermark, voter set) — per tech-spec §2.1.5 the initial implementation routes reads through the log for safety, meaning the returned state reflects the latest HW-committed position in the log and the caller can determine which entries are committed (offset < HW); linearisable reads (read-index, lease-based) are out of scope per tech-spec §2.2; the high_watermark in the returned state is an exclusive upper bound (entry at offset O is committed when O < HW, per architecture §3.1)
- [ ] Reject `propose()` with `NotLeader` error if the node is not the current leader

#### Test Scenarios
- [ ] Scenario: HW advancement — Given a 3-node cluster where leader has `log_end_offset=6` and both followers have `fetch_offset=6` (meaning they've fetched entries [0,6)), When HW is calculated by sorting [6,6,6] descending and taking index ⌊3/2⌋=1, Then HW = 6 (exclusive upper bound: entries 0–5 are committed because 5 < 6)
- [ ] Scenario: Propose commit notification — Given a client calls `propose(cmd)`, When the entry is appended and HW advances past it after follower fetches, Then the returned Future resolves with the committed offset
- [ ] Scenario: Two-round commit visibility — Given follower N2 fetches and replicates entry at offset 10, When N2 fetches again (round 2), Then the second response shows HW ≥ 11 (exclusive upper bound: entry 10 is committed because 10 < 11) and N2 executes the three-phase callback: (1) `StateMachine::apply` for the newly committed entry, (2) `Listener::handle_commit` with the batch, (3) `DeferredCompletionQueue::complete`
- [ ] Scenario: Propose on non-leader — Given follower N2, When `propose()` is called, Then it returns `NotLeader` error immediately

---

## Phase 6: Persistence, Crash Recovery, and Bootstrap (`xraft-core`)

> **Goal:** Implement the full crash-recovery sequence, cluster bootstrap,
> and ensure all consensus state transitions persist durably before
> acknowledgement. Depends on Phase 4 and Phase 5.
>
> **Sequencing:** Stages 6.1 and 6.2 are sequential.

### Stage 6.1: Crash Recovery Sequence

#### Implementation Steps
- [ ] Implement `RaftNode::recover()` entry point — called on startup before accepting RPCs; orchestrates the recovery phases below; the node starts in `Unattached` role during recovery and transitions to `Follower` upon completion, per architecture §5.9/§5.10
- [ ] Implement recovery phase 1: load quorum-state file for `current_term` and `voted_for` via `QuorumStateStore::load()`; if no file exists, initialise term=0 with no vote
- [ ] Implement recovery phase 2: load latest snapshot (if any) via `SnapshotIO::load_latest()`; call `StateMachine::restore(app_snapshot)` and restore voter set from `snapshot.metadata.voters`; set `log_start_offset` to `snapshot.metadata.last_included_offset + 1` (or 0 if no snapshot)
- [ ] Implement recovery phase 3: initialise `high_watermark` to `snapshot.metadata.last_included_offset + 1` (or 0 if no snapshot) — HW is never persisted (architecture §5.10 rule 1); entries [0, HW) are known committed from the snapshot; entries beyond HW have unknown committed status
- [ ] Implement recovery phase 4: scan log from `log_start_offset` to `log_end_offset`, validate CRC-32C per batch, truncate at first corrupt/partial record; do NOT apply any entries to the `StateMachine` (architecture §5.10 rule 2 — committed status unknown; entries may be uncommitted tail from a deposed leader)
- [ ] Implement recovery phase 5: process control records in the recovered log for bookkeeping only — `VotersRecord` → update voter set, `LeaderChangeMessage` → update leader-epoch checkpoint; these are idempotent consensus metadata updates, not state machine mutations; rebuild full leader-epoch checkpoint from log scan
- [ ] Implement recovery phase 6: transition role from `Unattached` to `Follower`, start election timer, begin accepting RPCs — a recovering node never resumes as Leader regardless of prior role; it must win a new election
- [ ] After recovery, the node sends Fetch requests to the leader; the leader's Fetch response includes the authoritative HW; the node advances local HW to `min(leader_HW, local_log_end_offset)` and executes the three-phase commit notification for entries between old HW and new HW per architecture §4.1: (1) `StateMachine::apply` for each `Command` entry (control records were already processed for bookkeeping during recovery phase 5), (2) `Listener::handle_commit` with batch of newly committed `AppRecord` values, (3) `DeferredCompletionQueue::complete`
- [ ] Ensure log recovery scan (from Stage 2.2) truncates any partially-written entries before replay
- [ ] Validate invariants after recovery: `log_start_offset ≤ high_watermark ≤ log_end_offset`, voter set non-empty (from snapshot or bootstrap VotersRecord), term ≥ snapshot term, HW = `snapshot.metadata.last_included_offset + 1` (not `log_end_offset`) — entries between HW and log_end_offset have unknown committed status and must not be applied to the state machine until the leader provides the authoritative HW

#### Test Scenarios
- [ ] Scenario: Clean recovery — Given a node with quorum-state(term=5, voted_for=N1), a snapshot at `last_included_offset=100`, and log entries 101–150, When the node restarts, Then it recovers to term 5 via `Unattached` → `Follower` transition with `high_watermark=101` (`snapshot.metadata.last_included_offset + 1`, not log_end_offset), log entries 101–150 are present on disk but NOT applied to the state machine (committed status unknown), control records in 101–150 are processed for bookkeeping only (VotersRecord → voter set, LeaderChangeMessage → epoch checkpoint), and the node begins sending Fetch requests to learn the authoritative HW from the leader
- [ ] Scenario: Recovery after crash mid-write — Given a node that crashed while appending entry 151 (corrupt CRC), When it restarts, Then entries 0–150 are recovered, entry 151 is discarded, and HW is set to `snapshot.metadata.last_included_offset + 1` (not 151)
- [ ] Scenario: Recovery with no snapshot — Given a node with log entries 0–50 and no snapshot, When it restarts, Then HW is set to 0 (no snapshot → no entries known committed), entries 0–50 are present on disk but NOT applied to the state machine; the node transitions `Unattached` → `Follower` and learns the authoritative HW from the leader via Fetch, at which point the three-phase commit notification executes for entries up to the leader-provided HW

### Stage 6.2: Cluster Bootstrap

#### Implementation Steps
- [ ] Implement `RaftNode::bootstrap(cluster_id: ClusterId, initial_voters: Vec<VoterInfo>)` — for first-time cluster formation per architecture §5.9: accept a shared `ClusterId` (generated once by the operator and distributed out-of-band to all nodes), store `cluster_id` and voter set in memory, set term=0 with no vote, transition role from `Unattached` to `Follower`, start election timer — the initial `VotersRecord` is NOT written to the log during bootstrap; the leader appends `LeaderChangeMessage` and `VotersRecord` to the log after winning the first election (architecture §5.9 bootstrap flow); the quorum-state file is first persisted when the node votes during the first election (via `QuorumStateStore::save` in Stage 4.2's fsync-before-ack rule)
- [ ] Implement bootstrap guard: reject `bootstrap()` if log is non-empty OR quorum-state file already exists OR a snapshot already exists — all three conditions indicate prior initialisation and must prevent re-bootstrap
- [ ] Implement single-node bootstrap for development/testing: a 1-node cluster follows the same election path — bootstrap to `Follower`, election timeout fires, node self-votes (persisting quorum-state), wins election as the only voter, becomes Leader, and appends `LeaderChangeMessage` + `VotersRecord` — this preserves the persistence contract that quorum-state is first written during the first election
- [ ] Connect `RaftNode::new(config, log_store, quorum_state_store, snapshot_io, transport_sender, transport_receiver, clock, state_machine, listener)` constructor that starts the node in `Unattached` role, calls `recover()` if data exists, or waits for `bootstrap()` if data directory is empty

#### Test Scenarios
- [ ] Scenario: Fresh bootstrap — Given a 3-node cluster with empty data directories and a shared `ClusterId` generated once, When each node calls `bootstrap(cluster_id, [N1, N2, N3])` with the same `ClusterId`, Then all nodes transition from `Unattached` to `Follower` with matching `ClusterId` and in-memory voter set {N1, N2, N3}; the log is empty and no quorum-state file exists until the first election occurs and the winning leader appends LeaderChangeMessage + VotersRecord
- [ ] Scenario: Double bootstrap rejection — Given a node that has already bootstrapped (log, quorum-state, or snapshot exists), When `bootstrap()` is called again, Then it returns an error
- [ ] Scenario: Single-node bootstrap — Given a 1-node cluster, When `bootstrap(cluster_id, [N1])` is called, Then N1 transitions from `Unattached` to `Follower`; when the election timeout expires, N1 self-votes (persisting quorum-state via fsync), wins election as the only voter, becomes Leader, and appends LeaderChangeMessage + VotersRecord to the log

---

## Phase 7: Log Compaction and Snapshot Transfer (`xraft-core`)

> **Goal:** Implement periodic snapshotting, log truncation after
> snapshot, and chunked snapshot transfer via `FetchSnapshot` RPC for
> followers that fall behind the log start offset. Depends on Phase 5
> (replication) and Phase 2 (snapshot store).
>
> **Sequencing:** Stages 7.1 and 7.2 are sequential.

### Stage 7.1: Periodic Snapshotting and Log Truncation

#### Implementation Steps
- [ ] Create `xraft-core/src/snapshot_coordinator.rs` implementing `SnapshotCoordinator`
- [ ] Implement snapshot trigger: after every N committed entries (configurable `snapshot_interval`), initiate a snapshot
- [ ] Implement snapshot creation: call `StateMachine::snapshot()` for app payload, combine with consensus metadata into `Snapshot { metadata: SnapshotMetadata { last_included_offset, last_included_term, voters, leader_epoch }, app_snapshot }`, write via `SnapshotIO::save` (architecture §4.1 method name)
- [ ] Implement log prefix truncation after snapshot: call `LogStore::truncate_prefix(snapshot.metadata.last_included_offset + 1)` to reclaim disk space — `log_start_offset` advances to `last_included_offset + 1` because the snapshot includes the entry at `last_included_offset`
- [ ] Ensure snapshot is fully persisted (fsync) before truncating any log entries

#### Test Scenarios
- [ ] Scenario: Automatic snapshot — Given `snapshot_interval = 100`, When 100 entries are committed, Then a snapshot is taken at `last_included_offset = 99` and the log prefix is truncated so `log_start_offset = 100`
- [ ] Scenario: Log reclamation — Given a snapshot at `last_included_offset = 499`, When prefix truncation runs, Then `log_start_offset() = 500` and entries 0–499 are no longer on disk
- [ ] Scenario: Snapshot before truncation — Given a snapshot write that fails (simulated IO error), When truncation is attempted, Then truncation is skipped (no data loss)

### Stage 7.2: Snapshot Transfer via FetchSnapshot RPC

#### Implementation Steps
- [ ] Implement `FetchSnapshot` handling on leader: serve snapshot chunks via `SnapshotIO::read_chunk` with position tracking
- [ ] Implement `FetchSnapshot` flow on follower: triggered when `Fetch` response contains `snapshot_id`, use `SnapshotIO::begin_receive(snapshot_id) -> SnapshotWriter` to initiate the chunked receive session, send `FetchSnapshotRequest` with position=0, write chunks via `SnapshotWriter`, finalize when `is_last_chunk=true`
- [ ] After snapshot is fully received: call `StateMachine::restore(snapshot.app_snapshot)` to restore application state, call `Listener::handle_load_snapshot(reader)` to notify the application (architecture §4.1 `Listener` trait), update voter set from `snapshot.metadata.voters`, set `log_start_offset` to `snapshot.metadata.last_included_offset + 1`, set `high_watermark` to `snapshot.metadata.last_included_offset + 1` (snapshot captures only committed state — consistent with recovery HW rule from architecture §5.10), set `fetch_offset` to `snapshot.metadata.last_included_offset + 1`, resume normal Fetch replication from the offset immediately after the snapshot (not the snapshot's last offset)
- [ ] Handle interrupted transfer: if the follower restarts mid-transfer, detect partial snapshot and restart from position 0

#### Test Scenarios
- [ ] Scenario: Full snapshot transfer — Given leader has snapshot at `last_included_offset=1000` and follower has empty log, When follower sends Fetch(offset=0) and gets `snapshot_id`, Then follower downloads the full snapshot via FetchSnapshot RPCs (using `begin_receive` / `SnapshotWriter`) and resumes fetching from offset 1001 (`snapshot.metadata.last_included_offset + 1`)
- [ ] Scenario: Chunked transfer integrity — Given a 50 KB snapshot transferred in 4 KB chunks, When all chunks are received and assembled, Then the reconstructed snapshot matches the original byte-for-byte
- [ ] Scenario: Interrupted transfer — Given a snapshot transfer interrupted after 5 of 13 chunks, When the follower reconnects, Then it restarts the transfer from position 0

---

## Phase 8: Dynamic Quorum (Membership Changes)

> **Goal:** Implement single-node-at-a-time voter add/remove/update,
> observer promotion, and leader step-down on removal. Depends on
> Phase 5 (replication, since VotersRecord must be committed),
> Phase 6 (bootstrap, since the initial voter set is established there),
> and Phase 7 (snapshot transfer, since observers that join an established
> cluster may need snapshot transfer if they are behind the leader's
> `log_start_offset`).
>
> **Sequencing:** Stages 8.1–8.3 are sequential (each builds on the prior).

### Stage 8.1: Membership Manager — Add Voter

#### Implementation Steps
- [ ] Create `xraft-core/src/membership.rs` implementing `MembershipManager`
- [ ] Implement `handle_add_voter(request)` on leader: validate no other membership change is in-flight, validate node is not already a voter, append `VotersRecord` control entry to log — the `VotersRecord` is committed using the **current** voter set for quorum (i.e., the new voter does NOT count towards the majority needed to commit this VotersRecord, per architecture §5.5)
- [ ] Implement observer registration: new node joins as observer (non-voting), replicates log via Fetch, does not contribute to quorum
- [ ] Track observer catch-up progress: observer must reach within a configurable threshold of the leader's log end before promotion
- [ ] On `VotersRecord` commit: update the in-memory voter set, begin counting the new voter for quorum calculations
- [ ] Return `MembershipChangeResponse` with appropriate errors: `NotLeader { leader_id }` (with `leader_id` field for client redirection), `ChangeInProgress`, `NodeAlreadyVoter`, `NodeNotFound`, `NodeNotCaughtUp` — all five variants from `MembershipError` enum defined in architecture §3.2

#### Test Scenarios
- [ ] Scenario: Add voter — Given a 3-node cluster {N1, N2, N3}, When leader N1 processes `AddVoter(N4)`, Then N4 is added as observer, catches up, a `VotersRecord` is committed, and N4 becomes a voter (quorum changes from 2 to 3)
- [ ] Scenario: Concurrent change rejected — Given a membership change is in progress, When a second `AddVoter` is requested, Then it returns `ChangeInProgress` error
- [ ] Scenario: Observer catch-up — Given N4 joins as observer with empty log and leader has 1000 entries, When N4 fetches and reaches within 10 entries of the leader, Then N4 is eligible for promotion
- [ ] Scenario: Observer not caught up — Given N4 joins as observer but is 500 entries behind the leader, When `AddVoter(N4)` attempts promotion, Then it returns `MembershipError::NodeNotCaughtUp`

### Stage 8.2: Membership Manager — Remove Voter

#### Implementation Steps
- [ ] Implement `handle_remove_voter(request)` on leader: validate no other change in-flight, validate node exists in voter set, append `VotersRecord` without the removed node — the `VotersRecord` is committed using the **new** voter set for quorum (i.e., the removed voter does NOT count towards the majority, per architecture §5.6), ensuring the new configuration is durable on a majority of the new membership before taking effect
- [ ] On `VotersRecord` commit: update in-memory voter set, exclude removed node from quorum calculations
- [ ] Handle leader self-removal: if the leader is the removed node, continue serving (processing Fetch requests, advancing HW) until the `VotersRecord` commits, then step down to `Unattached` (not `Follower`, because the leader is no longer a member of the voter set — per architecture §5.6 leader self-removal rules)
- [ ] Handle removed follower: when the removed node learns of its removal (by fetching and applying the `VotersRecord` that excludes it), it transitions to `Unattached` and stops participating in elections; Pre-Vote and voter-set-aware vote rejection prevent the removed node from disrupting the cluster (architecture §5.6)

#### Test Scenarios
- [ ] Scenario: Remove voter — Given a 5-node cluster, When leader removes N5, Then the `VotersRecord` [N1,N2,N3,N4] is committed using a majority of the **new** voter set (3 of 4), and quorum is then calculated over 4 nodes
- [ ] Scenario: Leader self-removal — Given N1 is leader of {N1, N2, N3}, When `RemoveVoter(N1)` is committed (using majority of new set {N2, N3}), Then N1 steps down to `Unattached` (not Follower) and a new election among {N2, N3} occurs
- [ ] Scenario: Remove non-existent node — Given voter set {N1, N2, N3}, When `RemoveVoter(N4)` is requested, Then `MembershipError::NodeNotFound` error is returned
- [ ] Scenario: Removed node behaviour — Given N3 is removed from voter set, When N3 fetches and applies the `VotersRecord` excluding it, Then N3 transitions to `Unattached` and stops participating in elections

### Stage 8.3: Update Voter and VotersRecord Snapshot Integration

#### Implementation Steps
- [ ] Implement `handle_update_voter(request)` on leader: update the endpoint of an existing voter in the voter set, append new `VotersRecord` to log
- [ ] Ensure voter set is included in snapshot metadata (`SnapshotMetadata.voters`) so that the voter set is recovered on restart
- [ ] Verify that after loading a snapshot during recovery, the voter set matches the last committed `VotersRecord`
- [ ] Add integration test covering: bootstrap → add voter → snapshot → remove voter → snapshot → recover from second snapshot

#### Test Scenarios
- [ ] Scenario: Update voter endpoint — Given N2 is a voter with endpoint `127.0.0.1:5002`, When `UpdateVoter(N2, 127.0.0.1:6002)` is committed, Then N2's endpoint in the voter set is updated
- [ ] Scenario: Voter set in snapshot — Given a cluster that has added N4 and N5 since bootstrap, When a snapshot is taken and the node recovers from it, Then the recovered voter set is {N1, N2, N3, N4, N5}

---

## Phase 9: Test Harness (`xraft-test`)

> **Goal:** Build the deterministic simulation harness for comprehensive
> scenario testing. The in-memory storage backends and `SimulatedClock`
> are defined in Stage 1.8 (Phase 1) to resolve the dependency ordering.
> This phase builds the higher-level `SimulatedCluster` and
> `InvariantChecker` on top of those primitives.
>
> **Sequencing:** Stage 9.1 depends on Phases 4–8 (consensus logic)
> plus Stage 1.8 (in-memory backends). Stage 9.2 depends on Stage 9.1.

### Stage 9.1: Simulated Cluster

#### Implementation Steps
- [ ] Create `xraft-test/src/simulated_cluster.rs` implementing `SimulatedCluster` — manages N `RaftNode` instances with `ChannelTransport`, `MemoryLogStore`, and `SimulatedClock`
- [ ] Implement `SimulatedCluster::new(n)` — bootstrap an N-node cluster with deterministic configuration
- [ ] Implement cluster control methods: `stop_node(id)`, `restart_node(id)`, `partition(set_a, set_b)`, `heal_partition()`, `advance_time(duration)`
- [ ] Implement `SimulatedCluster::propose(value)` — submit a proposal to the leader and return a future for its commit
- [ ] Implement `SimulatedCluster::wait_for_leader(timeout)` — advance simulated time until a leader is elected

#### Test Scenarios
- [ ] Scenario: Cluster formation — Given `SimulatedCluster::new(3)`, When time advances past the election timeout, Then exactly one leader is elected
- [ ] Scenario: Node stop and restart — Given a 3-node cluster with leader N1, When N1 is stopped and time advances, Then a new leader is elected among N2/N3; when N1 restarts, Then it rejoins as Follower

### Stage 9.2: Invariant Checker

#### Implementation Steps
- [ ] Create `xraft-test/src/invariant_checker.rs` implementing `InvariantChecker`
- [ ] Implement check: at most one leader per term across all nodes
- [ ] Implement check: log matching — if two nodes have an entry at the same index and term, all preceding entries match
- [ ] Implement check: leader completeness — elected leader's log contains all previously committed entries
- [ ] Implement check: state machine safety — no two nodes have applied different entries at the same index
- [ ] Wire `InvariantChecker` into `SimulatedCluster` to run after every state transition (configurable)

#### Test Scenarios
- [ ] Scenario: Invariant pass — Given a healthy 3-node cluster after 100 proposals, When `InvariantChecker` runs, Then all five Raft invariants pass
- [ ] Scenario: Invariant violation detection — Given a deliberately buggy `ElectionManager` that allows two leaders in the same term (injected for testing), When `InvariantChecker` runs, Then it panics with "at most one leader per term" violation

---

## Phase 10: Integration Testing and Scenario Validation

> **Goal:** Execute the end-to-end scenarios from `e2e-scenarios.md`
> using the deterministic simulation harness. Validate all safety
> properties under adversarial conditions. Depends on Phase 9.
>
> **Sequencing:** All stages in this phase are parallel-safe (independent
> test suites).

### Stage 10.1: Election Scenario Tests

#### Implementation Steps
- [ ] Implement test: initial leader election in 3-node and 5-node clusters
- [ ] Implement test: split vote with re-election
- [ ] Implement test: candidate with stale term is rejected
- [ ] Implement test: candidate with incomplete log is rejected
- [ ] Implement test: Pre-Vote prevents disruptive election by partitioned node
- [ ] Implement test: leader step-down on higher term

#### Test Scenarios
- [ ] Scenario: 3-node election — Given a fresh 3-node `SimulatedCluster`, When time advances past election timeout, Then exactly one node becomes leader and invariant checks pass
- [ ] Scenario: Pre-Vote partition — Given N3 partitioned from leader N1, When N3's election timeout expires, Then N3 cannot win pre-votes and does not disrupt the cluster's term

### Stage 10.2: Replication and Commit Scenario Tests

#### Implementation Steps
- [ ] Implement test: propose 100 entries and verify all committed across all nodes
- [ ] Implement test: two-round commit visibility (follower sees HW update on second fetch)
- [ ] Implement test: leader failure during replication — new leader continues from committed state
- [ ] Implement test: log divergence and truncation after leader change
- [ ] Implement test: concurrent proposals with high watermark advancement

#### Test Scenarios
- [ ] Scenario: Full replication — Given 100 proposals to a 3-node cluster, When all are committed, Then all 3 nodes have identical logs and invariant checks pass
- [ ] Scenario: Leader failure recovery — Given leader N1 fails after committing 50 entries, When a new leader is elected, Then entries 0–50 are preserved and new proposals succeed

### Stage 10.3: Partition, Snapshot, and Membership Scenario Tests

#### Implementation Steps
- [ ] Implement test: network partition and heal — verify Check Quorum step-down and re-election after healing
- [ ] Implement test: snapshot transfer to a far-behind follower
- [ ] Implement test: add voter to a running cluster and verify quorum change
- [ ] Implement test: remove voter (including leader self-removal)
- [ ] Implement test: crash recovery from snapshot — verify node transitions `Unattached` → `Follower` with HW = `snapshot.metadata.last_included_offset + 1`, does NOT apply post-snapshot entries to SM during recovery, learns authoritative HW from leader via Fetch, then applies committed entries
- [ ] Implement test: remove voter and verify removed node transitions to `Unattached` (not Follower)
- [ ] Implement test: stress test — 5-node cluster, 1000 proposals, random node crashes and restarts, random partitions, assert all invariants hold

#### Test Scenarios
- [ ] Scenario: Partition and heal — Given a 5-node cluster with leader N1, When {N1} is partitioned from {N2,N3,N4,N5}, Then N1 steps down (Check Quorum), a new leader is elected in the majority partition, and after healing all nodes converge
- [ ] Scenario: Stress test — Given 5 nodes, 1000 proposals, and random fault injection, When the test completes, Then all five Raft invariants hold and all committed entries are present on a majority of nodes

---

## Phase 11: Observability, Documentation, and Polish

> **Goal:** Add metrics, API documentation, README, and final quality
> gates. Depends on all prior phases.
>
> **Sequencing:** All stages are parallel-safe.

### Stage 11.1: Metrics Collector

#### Implementation Steps
- [ ] Create `xraft-core/src/metrics.rs` implementing `MetricsCollector` struct with fields: `current_leader`, `current_epoch`, `election_latency_avg`, `append_records_rate`, `commit_latency_avg`
- [ ] Wire metrics collection into event loop: update on leader election, HW advancement, proposal append
- [ ] Expose `RaftNode::metrics()` method returning a snapshot of current metrics
- [ ] Add integration test asserting metric values (not just existence) after known operations

#### Test Scenarios
- [ ] Scenario: Leader metric — Given a 3-node cluster after election, When `metrics()` is called on any node, Then `current_leader` matches the actual leader's NodeId
- [ ] Scenario: Commit latency — Given 10 proposals committed, When `metrics().commit_latency_avg` is read, Then it returns a non-zero positive duration

### Stage 11.2: API Documentation and README

#### Implementation Steps
- [ ] Add `#![deny(missing_docs)]` to all four crate roots
- [ ] Write doc-comments for `xraft-core` domain types: `NodeId`, `Term`, `ClusterId`, `Offset`, `LogEntry`, `EntryType`, `VoterInfo`, `VotersRecord`, `ConsensusState`, `QuorumState`, `FollowerProgress`, `AppRecord`, `AppSnapshot`, `Snapshot`, `SnapshotMetadata`, `SnapshotReader`, `SnapshotWriter` in their respective modules
- [ ] Write doc-comments for `xraft-core` traits: `LogStore`, `QuorumStateStore`, `SnapshotIO`, `TransportSender`, `TransportReceiver`, `Clock`, `StateMachine`, `Listener` and all trait methods in `traits.rs` and `listener.rs`
- [ ] Write doc-comments for `xraft-core` public API: `RaftNode` methods (`new`, `propose`, `read`, `bootstrap`, `shutdown`), `RaftConfig` fields, `XraftError` variants, RPC message types, and `IoAction` variants
- [ ] Write doc-comments for `xraft-storage` public API: `StorageEngine`, `SegmentLog`, `SnapshotStore`, `QuorumStateFile`, `LeaderEpochCheckpoint`
- [ ] Write doc-comments for `xraft-transport` public API: `TcpTransport`, `ChannelTransport`, `RpcCodec`, `NetworkSimulator`
- [ ] Write doc-comments for `xraft-test` public API: `SimulatedCluster`, `SimulatedClock`, `MemoryLogStore`, `InvariantChecker`
- [ ] Update root `README.md` with: project description, architecture overview, quick-start example, build/test instructions, and link to docs
- [ ] Add `examples/kv_store.rs` minimal key-value store example demonstrating `StateMachine` trait usage with `propose()` and `read()`

#### Test Scenarios
- [ ] Scenario: Docs build — Given all doc-comments are present, When `cargo doc --workspace --no-deps` is run, Then it exits with code 0 and no warnings
- [ ] Scenario: Example compiles — Given `examples/kv_store.rs`, When `cargo build --example kv_store` is run, Then it compiles successfully

### Stage 11.3: CI Pipeline and Release Readiness

#### Implementation Steps
- [ ] Verify CI pipeline runs: `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace -- -D warnings`, `cargo doc --workspace --no-deps`
- [ ] Add `cargo fmt --check` to CI pipeline
- [ ] Add `cargo audit` for dependency vulnerability scanning
- [ ] Run full integration test suite (Phase 10) in CI
- [ ] Verify all e2e scenarios from `e2e-scenarios.md` have corresponding test implementations

#### Test Scenarios
- [ ] Scenario: CI green — Given all code is committed, When CI runs, Then all build, test, clippy, fmt, and doc steps pass
- [ ] Scenario: E2E coverage — Given the list of scenarios in `e2e-scenarios.md`, When cross-referenced with test implementations, Then every scenario has at least one corresponding test


