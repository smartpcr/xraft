# Architecture: xraft — Raft Consensus Protocol in Rust

## 1. Architectural Overview

> **Greenfield notice.** The `smartpcr/xraft` repository contains no Rust
> source code as of this writing — only `README.md` and planning documents
> under `docs/`. Every crate name, module boundary, trait definition, and API
> signature described below is a **proposed design**. Nothing references
> existing code because none exists yet.

xraft is a proposed Rust library implementing the Raft consensus protocol
using a **pull-based (fetch) replication model** derived from Apache Kafka's
KRaft protocol. The design calls for a Cargo workspace of four crates that
enforce separation between the consensus state machine, durable storage,
network transport, and testing infrastructure.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Application Layer                            │
│           (implements StateMachine + Listener traits)                │
└───────────────────────────┬──────────────────────────────────────────┘
                            │ propose() / read() / callbacks
┌───────────────────────────▼──────────────────────────────────────────┐
│                   xraft-core  (proposed crate)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────────┐  │
│  │  RaftNode    │  │  EventLoop   │  │  ConsensusState           │  │
│  │  (public API)│──│  (single-    │──│  (term, role, voters,     │  │
│  │              │  │   threaded)  │  │   high watermark, log)    │  │
│  └──────────────┘  └──────┬───────┘  └───────────────────────────┘  │
│                           │                                          │
│  ┌──────────────┐  ┌──────▼───────┐  ┌───────────────────────────┐  │
│  │  Election    │  │  Replication  │  │  Membership               │  │
│  │  Manager     │  │  Manager     │  │  Manager                  │  │
│  └──────────────┘  └──────────────┘  └───────────────────────────┘  │
└──────────┬──────────────────┬────────────────────┬───────────────────┘
           │                  │                    │
┌──────────▼──────┐  ┌───────▼────────┐  ┌────────▼──────────────────┐
│ xraft-transport │  │ xraft-storage  │  │ xraft-test                │
│ (proposed crate)│  │(proposed crate)│  │ (proposed crate)          │
│ (async RPC)     │  │ (log, snap,    │  │ (deterministic simulation)│
│                 │  │  quorum-state) │  │                           │
└─────────────────┘  └────────────────┘  └───────────────────────────┘
```

**Design philosophy.** The proposed consensus core (`xraft-core`) is driven
by a single-threaded async event loop — no locks, no shared mutable state.
The event loop interacts with storage and network exclusively through
injected async trait objects (`LogStore`, `Transport`, `SnapshotIO`,
`QuorumStateStore`, `Clock`). It never opens files or sockets directly;
all concrete I/O is provided by the transport and storage crates at
construction time. This mirrors KRaft's `KafkaRaftClient` architecture
and eliminates concurrency bugs in the correctness-critical consensus
logic while allowing deterministic testing via trait substitution.

---

## 2. Components and Responsibilities

### 2.1 Proposed `xraft-core` — Consensus Engine

The central crate. Contains no direct I/O code — all storage and network
operations are dispatched through injected async trait objects. The event
loop `await`s trait methods but never opens files or sockets itself.

| Sub-component | Responsibility |
|---------------|----------------|
| **`RaftNode`** | Public API surface. Exposes `propose()`, `read()`, and lifecycle methods. Owns the `EventLoop` and coordinates startup/shutdown. Accepts a generic `StateMachine` type parameter (monomorphised at compile time). |
| **`EventLoop`** | Single-threaded async loop that drains an inbound message queue and dispatches to the appropriate handler. Every state transition happens here. Uses `tokio::sync::mpsc` for the inbound channel. Runs on a dedicated `tokio` task. Calls storage and transport through injected trait objects (e.g., `LogStore::append()`, `Transport::send()`); these calls are `await`ed but the loop never performs raw I/O itself. |
| **`ConsensusState`** | The core state: current `term`, `voted_for`, node `role` (Follower / Candidate / Leader), the in-memory log index, `high_watermark`, `log_start_offset`, the voter set, and per-follower replication progress (leader only). |
| **`ElectionManager`** | Implements Pre-Vote and Vote protocols. Manages election timeouts (randomised 150–300 ms), vote collection, term advancement, and leader-to-follower step-down on Check Quorum failure. |
| **`ReplicationManager`** | Handles Fetch request/response processing on both leader and follower sides. On the leader: validates fetch offset against the leader-epoch checkpoint, detects log divergence (populates `DivergingEpoch`), tracks follower progress, and advances the high watermark when a majority has replicated. On the follower: sends periodic Fetch RPCs, processes responses, truncates log on divergence, and updates the local high watermark. |
| **`MembershipManager`** | Processes `AddVoter` / `RemoveVoter` / `UpdateVoter` requests. Enforces single-node-at-a-time changes. Appends `VotersRecord` control entries to the log. Manages observer promotion to voter. Handles leader step-down when the leader is removed from the new configuration. |
| **`SnapshotCoordinator`** | Triggers periodic snapshots via the `StateMachine` trait. Coordinates `FetchSnapshot` RPC flows when a follower's required offset is below the log start offset. Manages chunked snapshot transfer state. |
| **`MetricsCollector`** | Maintains consensus metrics: `current_leader`, `current_epoch`, `election_latency_avg`, `append_records_rate`, `commit_latency_avg`. Exposed as a queryable Rust struct. |

### 2.2 Proposed `xraft-storage` — Durable Log and Snapshots

Owns all persistent state. Every write is `fsync`-ed before acknowledgement.

| Sub-component | Responsibility |
|---------------|----------------|
| **`SegmentLog`** | Append-only log stored as a series of segment files. Each segment covers a contiguous range of offsets. Supports append, read-range, truncate-suffix (for divergence), and truncate-prefix (for compaction after snapshot). Provides the `LogStore` trait implementation. |
| **`SegmentIndex`** | Sparse index mapping offset → file position within a segment. Enables `O(log n)` lookups for arbitrary offsets without scanning the full segment. |
| **`SnapshotStore`** | Manages snapshot files on disk. Writes are atomic (write-to-temp, fsync, rename). Supports chunked reads for `FetchSnapshot` transfer. Stores last-applied index, term, and voter set within the snapshot metadata. |
| **`QuorumStateFile`** | Persists voting state (`current_term`, `voted_for`, `leader_id`, `leader_epoch`) in a separate small file, analogous to KRaft's `quorum-state` file. Separated from the log for bootstrapping and performance. |
| **`LeaderEpochCheckpoint`** | Persists and caches the mapping from leader epoch → start offset. Used by the leader to validate Fetch requests and detect log divergence efficiently. Loaded into memory on startup. |

### 2.3 Proposed `xraft-transport` — Async RPC Layer

Abstracts network communication behind a trait so the core never touches
sockets directly. The proposed production implementation uses `tokio` TCP;
the proposed test implementation uses in-process channels.

| Sub-component | Responsibility |
|---------------|----------------|
| **`Transport` trait** | Defines `send(node_id, message) → Future<Result>` and `recv() → Stream<Message>`. Parameterised by message type. |
| **`TcpTransport`** | Production transport using `tokio::net::TcpStream`. Connections are pooled per peer. Messages are length-prefixed, serialised with `serde` + `bincode`. Each connection is multiplexed by RPC type. |
| **`ChannelTransport`** | In-process transport for integration tests. Uses `tokio::sync::mpsc` channels. Supports fault injection: message delay, drop, reorder, partition. |
| **`RpcCodec`** | Serialisation/deserialisation of RPC messages. Uses `serde` + `bincode`. Every message includes `cluster_id` and `leader_epoch` for identity verification and fencing. |

### 2.4 Proposed `xraft-test` — Deterministic Simulation Harness

Enables reproducible testing of edge cases that are impossible to trigger
reliably with wall-clock time and real I/O.

| Sub-component | Responsibility |
|---------------|----------------|
| **`SimulatedCluster`** | Manages a set of `RaftNode` instances running in-process. Wires them together with `ChannelTransport` and `MemoryLogStore`. Provides methods to start/stop nodes, inject faults, and assert invariants. |
| **`SimulatedClock`** | Deterministic clock injectable into the `EventLoop`. Advances only when explicitly ticked. Allows precise control over election timeouts, fetch intervals, and Check Quorum deadlines. |
| **`MemoryLogStore`** | In-memory implementation of the `LogStore` trait. Fast, deterministic, and supports fault injection (simulated `fsync` failure, write corruption). |
| **`NetworkSimulator`** | Controls message delivery in `ChannelTransport`. Supports scenarios: full partition, asymmetric partition, message loss with configurable probability, latency injection, and message reordering. |
| **`InvariantChecker`** | After each state transition, verifies the five Raft safety invariants: (1) at most one leader per term, (2) append-only leader log, (3) leader completeness, (4) log matching, (5) state machine safety. Panics on violation. |

---

## 3. Data Model

### 3.1 Core Entities

#### `NodeId`

```
NodeId {
    id: u64                         // unique numeric identifier within the cluster
}
```

#### `Term` (Epoch)

```
Term {
    value: u64                      // monotonically increasing logical clock
}
```

#### `LogEntry`

```
LogEntry {
    offset: u64                     // position in the log (0-indexed)
    term: Term                      // term when the entry was created
    entry_type: EntryType           // Command | NoOp | VotersRecord
    payload: Bytes                  // serialised command or control record
}
```

`EntryType` variants:
- **`Command`** — application-level state machine command.
- **`NoOp`** — blank entry committed by a new leader (`LeaderChangeMessage`
  equivalent) to establish commit state for the new term.
- **`VotersRecord`** — control record encoding a membership change.

#### `VotersRecord`

```
VotersRecord {
    version: u32
    voters: Vec<VoterInfo>
}

VoterInfo {
    node_id: NodeId
    endpoint: SocketAddr            // network address for RPC
}
```

Committed via the log as a `LogEntry` with `entry_type = VotersRecord`.
Included in snapshot metadata for recovery.

#### `ConsensusState`

```
ConsensusState {
    node_id: NodeId                 // this node's identity
    cluster_id: ClusterId           // cluster identity for fencing
    current_term: Term              // latest term this node has seen
    voted_for: Option<NodeId>       // candidate voted for in current term
    role: Role                      // Follower | Candidate | Leader
    leader_id: Option<NodeId>       // known leader (if any)

    // Log boundaries
    log_start_offset: u64           // first offset still in the log (after compaction)
    log_end_offset: u64             // next offset to be appended
    high_watermark: u64             // highest committed offset

    // Voter set (from latest VotersRecord or snapshot)
    voters: HashSet<NodeId>
    observers: HashSet<NodeId>

    // Leader-only state
    follower_state: HashMap<NodeId, FollowerProgress>

    // Election state
    election_deadline: Instant      // when to start election (follower/candidate)
    votes_received: HashSet<NodeId> // votes collected during election
    pre_votes_received: HashSet<NodeId>
    check_quorum_deadline: Instant  // leader: when to verify quorum
}
```

#### `FollowerProgress` (leader-side, per follower)

```
FollowerProgress {
    node_id: NodeId
    fetch_offset: u64               // last offset this follower has fetched
    last_fetch_timestamp: Instant   // when this follower last sent a Fetch
    is_voter: bool                  // whether this follower counts for quorum
}
```

#### `Snapshot`

```
SnapshotMetadata {
    last_included_offset: u64       // last log entry included in snapshot
    last_included_term: Term        // term of that entry
    voters: Vec<VoterInfo>          // voter set at snapshot time
}

Snapshot {
    metadata: SnapshotMetadata
    data: Vec<u8>                   // serialised state machine state
}
```

#### `QuorumState` (persisted voting state)

```
QuorumState {
    current_term: Term
    voted_for: Option<NodeId>
    leader_id: Option<NodeId>
    leader_epoch: Term
}
```

Persisted separately from the log in the `quorum-state` file.

### 3.2 RPC Messages

All messages include identity and fencing fields:

```
RpcEnvelope {
    cluster_id: ClusterId
    leader_epoch: Term
    source: NodeId
    payload: RpcPayload
}
```

#### `Vote` (Election)

```
VoteRequest {
    term: Term                      // candidate's term
    candidate_id: NodeId
    last_log_offset: u64            // candidate's last log offset
    last_log_term: Term             // term of candidate's last log entry
    is_pre_vote: bool               // true for Pre-Vote phase
}

VoteResponse {
    term: Term                      // responder's current term
    vote_granted: bool
    is_pre_vote: bool
}
```

#### `Fetch` (Log Replication)

```
FetchRequest {
    replica_id: NodeId              // follower/observer sending the request
    fetch_offset: u64               // offset the follower wants to read from
    last_fetched_epoch: Term        // epoch of the follower's last log entry
    max_bytes: u32                  // maximum response size
}

FetchResponse {
    leader_id: NodeId
    leader_epoch: Term
    high_watermark: u64             // current committed offset
    log_start_offset: u64           // leader's log start (after compaction)
    entries: Vec<LogEntry>          // log entries starting at fetch_offset

    // Set when log divergence is detected
    diverging_epoch: Option<DivergingEpoch>

    // Set when fetch_offset < log_start_offset (need snapshot)
    snapshot_id: Option<SnapshotId>
}

DivergingEpoch {
    epoch: Term                     // the epoch where divergence was found
    end_offset: u64                 // the offset to truncate to
}

SnapshotId {
    end_offset: u64                 // last offset included in snapshot
    epoch: Term                     // term of last entry in snapshot
}
```

#### `FetchSnapshot` (Snapshot Transfer)

```
FetchSnapshotRequest {
    snapshot_id: SnapshotId
    position: u64                   // byte offset into the snapshot file
    max_bytes: u32                  // maximum chunk size
}

FetchSnapshotResponse {
    snapshot_id: SnapshotId
    position: u64                   // byte offset of this chunk
    data: Bytes                     // chunk payload
    is_last_chunk: bool             // true if this is the final chunk
}
```

#### Membership Change RPCs

```
AddVoterRequest {
    node_id: NodeId
    endpoint: SocketAddr
}

RemoveVoterRequest {
    node_id: NodeId
}

UpdateVoterRequest {
    node_id: NodeId
    new_endpoint: SocketAddr
}

MembershipChangeResponse {
    success: bool
    leader_id: Option<NodeId>       // redirect if not the leader
    error: Option<MembershipError>
}

enum MembershipError {
    NotLeader { leader_id: Option<NodeId> }
    ChangeInProgress                // another change is uncommitted
    NodeAlreadyVoter
    NodeNotFound
    NodeNotCaughtUp                 // observer not yet at leader's HW
}
```

### 3.3 Segment File Layout

```
data/<cluster_id>/log/
├── 00000000000000000000.log        // segment: offsets 0–999
├── 00000000000000000000.index      // sparse index for segment 0
├── 00000000000000001000.log        // segment: offsets 1000–1999
├── 00000000000000001000.index
├── ...
├── snapshot/
│   └── <offset>-<term>.snap        // snapshot files
├── quorum-state                    // persisted voting state
└── leader-epoch-checkpoint         // epoch → start-offset mapping
```

Each `.log` segment file contains length-prefixed, bincode-serialised
`LogEntry` records. The `.index` file maps every Nth offset (configurable,
default 256) to the byte position in the `.log` file for fast seeks.

---

## 4. Interfaces Between Components

### 4.1 Trait Definitions

#### `StateMachine` (application → core)

```rust
pub trait StateMachine: Send + 'static {
    /// Apply a committed entry to the state machine.
    fn apply(&mut self, entry: &LogEntry) -> Result<()>;

    /// Take a snapshot of the current state.
    fn snapshot(&self) -> Result<Snapshot>;

    /// Restore state from a snapshot.
    fn restore(&mut self, snapshot: Snapshot) -> Result<()>;
}
```

Generic parameter on `RaftNode<SM: StateMachine>` — monomorphised at compile
time for zero-cost dispatch.

#### `Listener` (core → application callbacks)

```rust
pub trait Listener: Send + 'static {
    /// Called when a batch of entries is committed (HW advanced).
    fn handle_commit(&mut self, batch: &[LogEntry]);

    /// Called when a snapshot must be loaded (after FetchSnapshot completes).
    fn handle_load_snapshot(&mut self, reader: SnapshotReader);

    /// Called on leadership change.
    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term);

    /// Called during graceful shutdown.
    fn begin_shutdown(&mut self);
}
```

#### `LogStore` (core ↔ storage)

```rust
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append entries. Must fsync before returning Ok.
    async fn append(&mut self, entries: &[LogEntry]) -> Result<()>;

    /// Read entries in [start_offset, end_offset).
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;

    /// Truncate the log suffix starting at the given offset (for divergence).
    async fn truncate_suffix(&mut self, from_offset: u64) -> Result<()>;

    /// Truncate the log prefix up to the given offset (after snapshot).
    async fn truncate_prefix(&mut self, up_to_offset: u64) -> Result<()>;

    /// The first offset still in the log.
    fn log_start_offset(&self) -> u64;

    /// The next offset to be written.
    fn log_end_offset(&self) -> u64;

    /// Read the entry at the given offset.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}
```

#### `Transport` (core ↔ network)

```rust
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send a message to a specific node.
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;

    /// Receive the next inbound message.
    async fn recv(&mut self) -> Result<RpcEnvelope>;
}
```

#### `Clock` (core ↔ time)

```rust
pub trait Clock: Send + 'static {
    /// Current instant.
    fn now(&self) -> Instant;

    /// Sleep until the given deadline.
    async fn sleep_until(&self, deadline: Instant);

    /// Generate a random election timeout in [min, max].
    fn random_election_timeout(&self) -> Duration;
}
```

Production: wraps `tokio::time`. Test: `SimulatedClock` with manual tick.

#### `SnapshotIO` (core ↔ storage, for snapshots)

```rust
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Write a complete snapshot atomically.
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;

    /// Load the latest snapshot, if any.
    async fn load_latest(&self) -> Result<Option<Snapshot>>;

    /// Read a chunk of the snapshot at the given byte position.
    async fn read_chunk(&self, id: &SnapshotId, position: u64, max_bytes: u32)
        -> Result<(Bytes, bool)>; // (data, is_last_chunk)

    /// Begin writing a snapshot received from a leader, chunk by chunk.
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter>;
}
```

#### `QuorumStateStore` (core ↔ storage, for voting state)

```rust
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    /// Load persisted quorum state.
    async fn load(&self) -> Result<Option<QuorumState>>;

    /// Persist quorum state. Must fsync before returning Ok.
    async fn save(&self, state: &QuorumState) -> Result<()>;
}
```

### 4.2 Component Interaction Map

```
                     ┌────────────────────────────────────────────────┐
                     │              Application                       │
                     │ ┌──────────┐            ┌───────────┐          │
                     │ │StateMach.│            │ Listener  │          │
                     │ └─────┬────┘            └─────┬─────┘          │
                     └───────┼───────────────────────┼────────────────┘
                             │                       │
               propose()    │    apply/snapshot      │ handle_commit
               read()       │    restore             │ handle_leader_change
                             │                       │ handle_load_snapshot
                     ┌───────▼───────────────────────▼────────────────┐
                     │                 RaftNode                        │
                     │           ┌──────────────┐                     │
                     │           │  EventLoop   │                     │
                     │           │ (msg queue)  │                     │
                     │           └──────┬───────┘                     │
                     │    ┌─────────────┼────────────────┐            │
                     │    ▼             ▼                ▼            │
                     │ Election     Replication     Membership        │
                     │ Manager     Manager         Manager            │
                     │    │             │                │            │
                     └────┼─────────────┼────────────────┼────────────┘
                          │             │                │
            ┌─────────────┼─────────────┼────────────────┼─────────┐
            ▼             ▼             ▼                ▼         ▼
     ┌────────────┐ ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌────────┐
     │ Transport  │ │ LogStore │ │SnapshotIO │ │QuorumSt. │ │ Clock  │
     │  (trait)   │ │ (trait)  │ │  (trait)  │ │  (trait) │ │(trait) │
     └─────┬──────┘ └────┬─────┘ └─────┬─────┘ └────┬─────┘ └───┬────┘
           │              │             │             │           │
           ▼              ▼             ▼             ▼           ▼
     ┌──────────┐  ┌───────────┐ ┌───────────┐ ┌──────────┐ ┌────────┐
     │TcpTrans- │  │SegmentLog │ │ Snapshot-  │ │QuorumSt- │ │Tokio-  │
     │port      │  │           │ │ Store      │ │ateFile   │ │Clock   │
     │(or Chan- │  │(+Segment  │ │            │ │          │ │(or Sim-│
     │nelTrans.)│  │  Index)   │ │            │ │          │ │ulated) │
     └──────────┘  └───────────┘ └───────────┘ └──────────┘ └────────┘
```

### 4.3 Proposed Crate Dependency Graph

```
xraft-test ──depends-on──► xraft-core
xraft-test ──depends-on──► xraft-transport  (ChannelTransport)
xraft-test ──depends-on──► xraft-storage    (MemoryLogStore)

xraft-core ──depends-on──► (trait definitions only — no concrete I/O deps)

xraft-transport ──depends-on──► xraft-core  (message types, NodeId)
xraft-storage   ──depends-on──► xraft-core  (LogEntry, Snapshot, QuorumState types)
```

`xraft-core` will define all trait interfaces and data types. The concrete
implementations in `xraft-transport` and `xraft-storage` will depend on
`xraft-core` for those types. `xraft-test` will depend on all three to
wire up simulation clusters.

---

## 5. End-to-End Sequence Flows

### 5.1 Leader Election (with Pre-Vote)

Triggered when a follower's election timeout expires without receiving a
Fetch response from a leader.

```
    Follower A              Follower B              Follower C
        │                       │                       │
        │  (election timeout)   │                       │
        │                       │                       │
  ┌─────┴──────┐                │                       │
  │ Phase 1:   │                │                       │
  │ Pre-Vote   │                │                       │
  │ term=T+1   │                │                       │
  │ (no term   │                │                       │
  │  increment)│                │                       │
  └─────┬──────┘                │                       │
        │                       │                       │
        │── VoteRequest ───────►│                       │
        │   (is_pre_vote=true,  │                       │
        │    term=T+1)          │                       │
        │                       │                       │
        │── VoteRequest ────────┼──────────────────────►│
        │   (is_pre_vote=true)  │                       │
        │                       │                       │
        │◄─ VoteResponse ──────│                       │
        │   (granted=true)      │                       │
        │                       │                       │
        │◄─ VoteResponse ──────┼───────────────────────│
        │   (granted=true)      │                       │
        │                       │                       │
  ┌─────┴──────┐                │                       │
  │ Majority   │                │                       │
  │ pre-votes  │                │                       │
  │ received.  │                │                       │
  │ Phase 2:   │                │                       │
  │ Real Vote  │                │                       │
  │ term ← T+1 │                │                       │
  │ voted_for  │                │                       │
  │  ← self    │                │                       │
  │ persist    │                │                       │
  │ quorum-st  │                │                       │
  └─────┬──────┘                │                       │
        │                       │                       │
        │── VoteRequest ───────►│                       │
        │   (is_pre_vote=false, │                       │
        │    term=T+1)          │                       │
        │                       │                       │
        │── VoteRequest ────────┼──────────────────────►│
        │                       │                       │
        │◄─ VoteResponse ──────│                       │
        │   (granted=true)      │  (persist voted_for)  │
        │                       │                       │
        │◄─ VoteResponse ──────┼───────────────────────│
        │   (granted=true)      │                       │
        │                       │                       │
  ┌─────┴──────┐                │                       │
  │ Majority   │                │                       │
  │ votes:     │                │                       │
  │ ⌊3/2⌋+1=2 │                │                       │
  │ received 3 │                │                       │
  │ (self+B+C) │                │                       │
  │ → Become   │                │                       │
  │ LEADER     │                │                       │
  │ term=T+1   │                │                       │
  └─────┬──────┘                │                       │
        │                       │                       │
        │  Append NoOp entry    │                       │
        │  (LeaderChangeMessage)│                       │
        │  to own log, persist  │                       │
        │                       │                       │
```

**Key rules enforced:**
- A follower rejects a Pre-Vote if it has received a Fetch response from a
  valid leader within the election timeout (prevents disruptive elections).
- A follower grants a real Vote only if the candidate's log is at least as
  up-to-date as its own (last log term ≥ follower's last log term; if equal,
  last log offset ≥ follower's last log offset).
- `voted_for` and `current_term` are persisted via `QuorumStateStore::save()`
  before any Vote response is sent.

### 5.2 Log Replication (Pull-Based Fetch)

Steady-state replication. Followers periodically send Fetch RPCs to the
leader. Two fetch rounds are required for a follower to observe a commit.

```
    Client          Leader (A)          Follower B          Follower C
       │                │                    │                   │
       │── propose(cmd)►│                    │                   │
       │                │                    │                   │
       │          ┌─────┴──────┐             │                   │
       │          │ Append to  │             │                   │
       │          │ log @off=5 │             │                   │
       │          │ term=T     │             │                   │
       │          │ fsync      │             │                   │
       │          └─────┬──────┘             │                   │
       │                │                    │                   │
       │                │  ◄── FetchRequest ─│                   │
       │                │      (fetch_off=5, │                   │
       │                │       epoch=T)     │                   │
       │                │                    │                   │
       │                │── FetchResponse ──►│                   │
       │                │   entries=[off=5], │                   │
       │                │   HW=4 (not yet    │                   │
       │                │   committed)       │                   │
       │                │                    │                   │
       │                │                    │  ┌──────────┐     │
       │                │                    │  │Append 5  │     │
       │                │                    │  │fsync     │     │
       │                │                    │  └──────────┘     │
       │                │                    │                   │
       │                │  ◄── FetchRequest ─┼───────────────────│
       │                │      (fetch_off=5) │                   │
       │                │                    │                   │
       │                │── FetchResponse ───┼──────────────────►│
       │                │   entries=[off=5], │                   │
       │                │   HW=4             │                   │
       │                │                    │                   │
       │          ┌─────┴──────┐             │                   │
       │          │ B fetched  │             │                   │
       │          │ off=6, C   │             │                   │
       │          │ fetched 6. │             │                   │
       │          │ Offsets:   │             │                   │
       │          │ A=6,B=5,   │             │                   │
       │          │ C=5 sorted │             │                   │
       │          │ desc=[6,5, │             │                   │
       │          │ 5] idx     │             │                   │
       │          │ ⌊3/2⌋=1   │             │                   │
       │          │ → HW ← 5  │             │                   │
       │          └─────┬──────┘             │                   │
       │                │                    │                   │
       │                │  ◄── FetchRequest ─│  (second round)   │
       │                │      (fetch_off=6) │                   │
       │                │                    │                   │
       │                │── FetchResponse ──►│                   │
       │                │   entries=[],      │                   │
       │                │   HW=5 ◄── commit! │                   │
       │                │                    │                   │
       │◄── Ok(result) ─│                    │                   │
       │   (committed)  │                    │  apply(off=5)     │
       │                │                    │                   │
```

**High-watermark advancement rule (quorum math).** The leader maintains
`FollowerProgress` for each voter. When a Fetch request arrives from
follower F at `fetch_offset = N`, the leader records that F has replicated
up to `N-1`. To compute the new high watermark, the leader collects the
replicated offset for every voter (including itself) and sorts them in
**descending** order. The new high watermark is the value at index
`⌊V/2⌋` (0-indexed), where `V` is the total number of voters. This is
the highest offset replicated by at least a **majority** (`⌊V/2⌋ + 1`)
of voters.

*Example (V=3, offsets [10, 8, 5]):* Sorted descending: [10, 8, 5].
Index ⌊3/2⌋ = 1 → value 8. Two voters (10, 8) have replicated ≥ 8 →
majority. HW ← 8.

*Example (V=5, offsets [10, 8, 7, 5, 3]):* Sorted descending:
[10, 8, 7, 5, 3]. Index ⌊5/2⌋ = 2 → value 7. Three voters (10, 8, 7)
have replicated ≥ 7 → majority. HW ← 7.

*Example (V=4, offsets [10, 8, 5, 3]):* Sorted descending:
[10, 8, 5, 3]. Index ⌊4/2⌋ = 2 → value 5. Three voters (10, 8, 5)
have replicated ≥ 5 → majority. HW ← 5.

Only voters count; observers do not contribute to quorum.

**Two-round commit visibility:** A follower fetches entries in round 1 but
receives the old HW. The leader advances HW after majority replication. The
follower sees the new HW in round 2's response and can then apply committed
entries to the state machine.

### 5.3 Log Divergence Detection and Truncation

Occurs when a follower's log diverged from the leader's (e.g., after a
leader failure where the old leader had uncommitted entries).

```
    Leader (new, term=3)                Follower B
          │                                  │
          │  ◄─────── FetchRequest ──────────│
          │   fetch_offset=10,               │
          │   last_fetched_epoch=2           │
          │                                  │
    ┌─────┴──────┐                           │
    │ Validate:  │                           │
    │ Check      │                           │
    │ leader-    │                           │
    │ epoch-     │                           │
    │ checkpoint │                           │
    │            │                           │
    │ Epoch 2    │                           │
    │ ended at   │                           │
    │ offset 8   │                           │
    │ on leader. │                           │
    │ Follower   │                           │
    │ claims 10  │                           │
    │ → DIVERGE  │                           │
    └─────┬──────┘                           │
          │                                  │
          │──── FetchResponse ──────────────►│
          │  diverging_epoch = {             │
          │    epoch: 2,                     │
          │    end_offset: 8                 │
          │  }                               │
          │  entries = []                    │
          │                                  │
          │                            ┌─────┴──────┐
          │                            │ Truncate   │
          │                            │ log suffix │
          │                            │ from off=8 │
          │                            │ (discard   │
          │                            │  8,9)      │
          │                            │ fsync      │
          │                            └─────┬──────┘
          │                                  │
          │  ◄─────── FetchRequest ──────────│
          │   fetch_offset=8,               │
          │   last_fetched_epoch=1           │
          │                                  │
          │──── FetchResponse ──────────────►│
          │  entries=[off=8, off=9, ...]     │
          │  (leader's entries for epoch 3)  │
          │                                  │
```

The leader uses the **leader-epoch checkpoint** — an in-memory cache of
`(epoch → start_offset)` — to quickly determine the valid end offset for any
epoch. If the follower's claimed offset for an epoch exceeds the checkpoint
value, divergence is reported. Multiple rounds may be needed if the follower
diverged across several epochs.

### 5.4 Snapshot Transfer

Triggered when a follower's `fetch_offset` is below the leader's
`log_start_offset` (the leader has compacted the entries the follower needs).

```
    Leader                               Follower
      │                                      │
      │  ◄──────── FetchRequest ─────────────│
      │   fetch_offset=50                    │
      │                                      │
  ┌───┴────┐                                 │
  │ LSO=   │                                 │
  │ 1000.  │                                 │
  │ 50 <   │                                 │
  │ 1000   │                                 │
  │ → snap │                                 │
  └───┬────┘                                 │
      │                                      │
      │── FetchResponse ────────────────────►│
      │  entries=[],                         │
      │  snapshot_id={off=999, epoch=T}      │
      │                                      │
      │  ◄── FetchSnapshotRequest ───────────│
      │   snapshot_id={off=999, epoch=T},    │
      │   position=0, max_bytes=1MB          │
      │                                      │
      │── FetchSnapshotResponse ────────────►│
      │  data=[...chunk1...],                │
      │  is_last_chunk=false                 │
      │                                      │
      │  ◄── FetchSnapshotRequest ───────────│
      │   position=1048576                   │
      │                                      │
      │── FetchSnapshotResponse ────────────►│
      │  data=[...chunk2...],                │
      │  is_last_chunk=true                  │
      │                                      │
      │                                ┌─────┴──────┐
      │                                │ Atomic     │
      │                                │ snapshot   │
      │                                │ install:   │
      │                                │ 1. restore │
      │                                │    state   │
      │                                │    machine │
      │                                │ 2. update  │
      │                                │    log_    │
      │                                │    start   │
      │                                │ 3. update  │
      │                                │    voters  │
      │                                │ 4. fsync   │
      │                                └─────┬──────┘
      │                                      │
      │  ◄──────── FetchRequest ─────────────│
      │   fetch_offset=1000                  │
      │   (resume normal replication)        │
      │                                      │
```

### 5.5 Dynamic Membership Change (Add Voter)

Adding a new node to the cluster. The node first joins as an observer,
catches up via Fetch, then is promoted to voter.

```
    Admin         Leader           Observer D        Follower B
      │              │                 │                 │
      │              │  ◄── Fetch ─────│ (observer       │
      │              │                 │  replicating)   │
      │              │── FetchResp ───►│                 │
      │              │                 │                 │
      │  (observer D has caught up to HW)                │
      │              │                 │                 │
      │─ AddVoter ──►│                 │                 │
      │  (node=D,    │                 │                 │
      │   endpoint)  │                 │                 │
      │              │                 │                 │
      │        ┌─────┴──────┐          │                 │
      │        │ Validate:  │          │                 │
      │        │ 1. Am I    │          │                 │
      │        │    leader? │          │                 │
      │        │ 2. No      │          │                 │
      │        │    pending │          │                 │
      │        │    change? │          │                 │
      │        │ 3. D is    │          │                 │
      │        │    caught  │          │                 │
      │        │    up?     │          │                 │
      │        │ 4. Append  │          │                 │
      │        │    Voters- │          │                 │
      │        │    Record  │          │                 │
      │        │    to log  │          │                 │
      │        └─────┬──────┘          │                 │
      │              │                 │                 │
      │              │  ◄── Fetch ─────│                 │
      │              │── FetchResp ───►│                 │
      │              │  (includes      │                 │
      │              │   VotersRecord) │  ◄── Fetch ─────│
      │              │                 │                 │
      │              │── FetchResp ────┼────────────────►│
      │              │  (includes      │                 │
      │              │   VotersRecord) │                 │
      │              │                 │                 │
      │        ┌─────┴──────┐          │                 │
      │        │ Majority   │          │                 │
      │        │ fetched    │          │                 │
      │        │ VotersRec. │          │                 │
      │        │ HW adv.    │          │                 │
      │        │ D is now   │          │                 │
      │        │ a voter.   │          │                 │
      │        └─────┬──────┘          │                 │
      │              │                 │                 │
      │◄── Ok ───────│                 │                 │
      │              │                 │                 │
```

**Single-change invariant:** The leader rejects `AddVoter` / `RemoveVoter` /
`UpdateVoter` if there is already an uncommitted `VotersRecord` in the log.
This prevents disjoint majorities that could arise from concurrent membership
changes.

### 5.6 Check Quorum (Leader Liveness)

The leader periodically verifies it can still reach a majority of voters.

```
    Leader                 Follower B           Follower C
      │                       │                     │
      │  (check_quorum_       │                     │
      │   deadline fires)     │                     │
      │                       │                     │
  ┌───┴────────────┐          │                     │
  │ Scan follower  │          │                     │
  │ progress:      │          │                     │
  │ B: last_fetch  │          │                     │
  │    = 200ms ago │          │                     │
  │ C: last_fetch  │          │                     │
  │    = 5s ago    │          │                     │
  │    (> elect.   │          │                     │
  │     timeout)   │          │                     │
  │                │          │                     │
  │ Voters with    │          │                     │
  │ recent fetch:  │          │                     │
  │ {self, B} = 2  │          │                     │
  │ Majority =     │          │                     │
  │  ⌊3/2⌋+1 = 2  │          │                     │
  │ 2 ≥ 2 → OK    │          │                     │
  └───┬────────────┘          │                     │
      │                       │                     │
      │  (continue as leader) │                     │
      │                       │                     │

  --- If quorum is NOT met: ---

  ┌───┴────────────┐          │                     │
  │ Voters with    │          │                     │
  │ recent fetch:  │          │                     │
  │ {self} = 1     │          │                     │
  │ Majority =     │          │                     │
  │  ⌊3/2⌋+1 = 2  │          │                     │
  │ 1 < 2 → FAIL  │          │                     │
  │                │          │                     │
  │ Step down to   │          │                     │
  │ FOLLOWER       │          │                     │
  └───┬────────────┘          │                     │
      │                       │                     │
```

The leader counts itself plus every voter whose `last_fetch_timestamp` is
within the election timeout window. If the count is below majority
(`⌊V/2⌋ + 1` where V is the voter count), the leader steps down to
follower to prevent split-brain.

### 5.7 Client Proposal (Full Path)

End-to-end flow from client command to committed state machine application.
The `EventLoop` calls `LogStore` through the injected trait object (shown
as `LogStore (trait)`). No raw I/O occurs inside `xraft-core`.

```
    Client              RaftNode              EventLoop        LogStore (trait)
      │                    │                      │                    │
      │── propose(cmd) ───►│                      │                    │
      │                    │                      │                    │
      │                    │── Propose(cmd) ──────►│                    │
      │                    │   (via mpsc channel)  │                    │
      │                    │                       │                    │
      │                    │                 ┌─────┴──────┐            │
      │                    │                 │ Am I       │            │
      │                    │                 │ leader?    │            │
      │                    │                 │ YES        │            │
      │                    │                 │            │            │
      │                    │                 │ Create     │            │
      │                    │                 │ LogEntry   │            │
      │                    │                 │ off=N,     │            │
      │                    │                 │ term=T     │            │
      │                    │                 └─────┬──────┘            │
      │                    │                       │                    │
      │                    │                       │── .append(entry)──►│
      │                    │                       │   (await trait     │
      │                    │                       │    method)         │
      │                    │                       │◄── Ok (fsync'd) ──│
      │                    │                       │                    │
      │                    │                 ┌─────┴──────┐            │
      │                    │                 │ Register   │            │
      │                    │                 │ pending    │            │
      │                    │                 │ proposal   │            │
      │                    │                 │ (off=N,    │            │
      │                    │                 │  oneshot)  │            │
      │                    │                 └─────┬──────┘            │
      │                    │                       │                    │
      │                    │     ... followers fetch entry N ...       │
      │                    │     ... HW advances to N ...              │
      │                    │                       │                    │
      │                    │                 ┌─────┴──────┐            │
      │                    │                 │ HW ≥ N     │            │
      │                    │                 │ Apply to   │            │
      │                    │                 │ state mach.│            │
      │                    │                 │ Notify     │            │
      │                    │                 │ Listener   │            │
      │                    │                 │ Complete   │            │
      │                    │                 │ oneshot    │            │
      │                    │                 └─────┬──────┘            │
      │                    │                       │                    │
      │                    │◄── Result ────────────│                    │
      │                    │                       │                    │
      │◄── Ok(result) ────│                       │                    │
      │                    │                       │                    │
```

If the node is not the leader, `propose()` returns
`Err(NotLeader { leader_id })` so the client can redirect.

---

## 6. Cross-Cutting Concerns

### 6.1 Persistence Guarantees

Every write path that affects correctness calls `fsync` before
acknowledgement. The `EventLoop` invokes these operations through the
injected trait objects — the concrete `fsync` implementation lives in
`xraft-storage`, not in `xraft-core`:

| Write | When | Guarantee |
|-------|------|-----------|
| `QuorumStateStore::save()` | Before sending any Vote response; on term change | `current_term` + `voted_for` are durable |
| `LogStore::append()` | Before responding to any Fetch (leader) or after applying Fetch response (follower) | Log entries are durable before replication acknowledgement |
| `SnapshotIO::save()` | After state machine serialisation | Snapshot is durable before log truncation |

### 6.2 Fencing and Identity

Every `RpcEnvelope` carries `cluster_id` and `leader_epoch`. Receivers reject
messages with a mismatched `cluster_id` (prevents cross-cluster contamination)
or a stale `leader_epoch` (prevents acting on messages from a deposed leader).

### 6.3 Error Handling Strategy

| Error Category | Handling |
|----------------|----------|
| **Storage I/O failure** | `LogStore` and `SnapshotIO` return `Result`. The `EventLoop` logs the error, notifies the `Listener` via `begin_shutdown()`, and halts the node. Crash-stop is safer than operating with potentially corrupt state. |
| **Network failure** | `Transport::send()` failures are logged and retried on next tick. The pull-based model is inherently tolerant — a missed Fetch is equivalent to a slow follower. |
| **Deserialization failure** | Malformed RPC messages are dropped with a warning log. |
| **Stale term** | Any RPC with a term higher than `current_term` triggers a step-down to follower and term update. RPCs with a lower term are rejected. |

### 6.4 Metrics

Exposed as a queryable struct, not tied to a specific metrics backend.

```rust
pub struct RaftMetrics {
    pub current_leader: Option<NodeId>,
    pub current_epoch: u64,
    pub election_latency_avg_ms: f64,
    pub append_records_rate: f64,       // records/sec
    pub commit_latency_avg_ms: f64,
    pub high_watermark: u64,
    pub log_end_offset: u64,
    pub log_start_offset: u64,
    pub role: Role,
    pub voter_count: usize,
    pub observer_count: usize,
}
```

---

## 7. Alignment Notes

### Consistency with `tech-spec.md`

This architecture is designed to be fully consistent with the tech spec.
All crate names, trait definitions, and module structures below are
**proposed designs** — no Rust source code exists in the repository yet.

- **Proposed crate layout** matches §4.4 of the tech spec: `xraft-core`,
  `xraft-transport`, `xraft-storage`, `xraft-test`.
- **RPC names** match §2.1.4: `Vote`, `Fetch`, `FetchSnapshot`, `AddVoter`,
  `RemoveVoter`, `UpdateVoter`.
- **Pull-based replication** per §3 Non-Goals item 2 — no `AppendEntries`.
- **Serialisation** uses `serde` + `bincode` per §6 Key Design Decisions.
- **State machine interface** is generic (monomorphised) per §6.
- **Segment-file log storage** per §6.
- **I/O-abstract event loop** per §4.4 — the event loop `await`s injected
  trait objects for storage and transport; it never opens files or sockets
  directly. This enables deterministic testing via trait substitution
  (see `xraft-test`).
- **Timing parameters** (150–300 ms election timeout, 50 ms fetch interval)
  per §4.3.
- **Quorum math** — majority is `⌊V/2⌋ + 1`; HW advancement uses
  descending-sorted voter offsets at index `⌊V/2⌋`. Consistent with
  §2.1.1 of the tech spec.

### Open Items for Sibling Documents

- `implementation-plan.md`: This architecture defines WHAT to build.
  The implementation plan should sequence the work — likely core election →
  log replication → persistence → snapshot → membership → simulation harness.
- `e2e-scenarios.md`: Sequence flows §5.1–§5.7 here define the "happy path"
  and key failure modes. The E2E document should define concrete test
  scenarios with expected assertions for each flow.
