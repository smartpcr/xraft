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
│           (receives AppRecord only — never control records)         │
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
│                           │                                          │
│  ┌──────────────┐  ┌──────▼───────┐  ┌───────────────────────────┐  │
│  │  Batch       │  │  IoStage     │  │  DeferredCompletion       │  │
│  │  Accumulator │──│  (concurrent │  │  Queue (park/complete     │  │
│  │  (stage      │  │   I/O exec)  │  │   client futures)         │  │
│  │   proposals) │  │              │  │                           │  │
│  └──────────────┘  └──────┬───────┘  └───────────────────────────┘  │
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
The event loop processes protocol messages, mutates `ConsensusState`, and
invokes application callbacks (`StateMachine::apply`, `Listener`) as
synchronous in-process calls whenever a commit-visible event occurs (e.g.,
HW advancement). It then produces `IoAction` values that describe external
I/O to perform (append log, send RPC, persist quorum state). An `IoStage`
executes those actions concurrently via the injected async trait objects
(`LogStore`, `TransportSender`, `SnapshotIO`, `QuorumStateStore`). The
event loop never opens files or sockets directly; all concrete external I/O
is provided by the transport and storage crates at construction time.
Inbound messages arrive via an mpsc channel fed by a dedicated
`ReceiverTask` that calls `TransportReceiver::recv()` (see §4.4). The
event loop uses the injected `Clock` directly for timer management
(election timeouts, check-quorum deadlines) — `Clock` is not mediated by
`IoAction`. Application callbacks are not external I/O — they are
synchronous, in-process method calls (statically dispatched when the
generic type parameters are monomorphised) that execute within the event
loop's thread and always observe fully updated protocol state before any
`IoAction` is dispatched. Callbacks must be lightweight and non-blocking;
applications that need heavy processing should hand off work to their own
async tasks. Incoming
proposals are staged in a `BatchAccumulator` and drained on each tick;
client futures are parked in a `DeferredCompletionQueue` until the high
watermark advances past their offset. This mirrors KRaft's
`KafkaRaftClient` / `BatchAccumulator` / `DeferredEventQueue` architecture
and eliminates concurrency bugs in the correctness-critical consensus logic
while preventing slow I/O from delaying Fetch processing and triggering
spurious elections.

---

## 2. Components and Responsibilities

### 2.1 Proposed `xraft-core` — Consensus Engine

The central crate. Contains no direct I/O code — all storage and network
send operations are expressed as `IoAction` values produced by the event
loop and executed by the `IoStage`, which calls the injected async trait
objects (`LogStore`, `TransportSender`, `SnapshotIO`, `QuorumStateStore`).
Inbound messages arrive via an mpsc channel fed by the `ReceiverTask`
(§4.4). The event loop itself never opens files or sockets; it only
mutates in-memory `ConsensusState`, invokes synchronous application
callbacks (`StateMachine`, `Listener`), manages timers via the injected
`Clock`, and emits `IoAction` batches.

| Sub-component | Responsibility |
|---------------|----------------|
| **`RaftNode`** | Public API surface. Exposes `propose()`, `read()`, `bootstrap()`, and lifecycle methods. Owns the `EventLoop`, `ReceiverTask`, and `IoStage`; coordinates startup, shutdown, and crash recovery. Generic over two application-provided types: `S: StateMachine` and `L: Listener` (both monomorphised at compile time for zero-cost dispatch). I/O and runtime traits (`LogStore`, `TransportSender`, `TransportReceiver`, `QuorumStateStore`, `SnapshotIO`, `Clock`) are injected as trait objects (`Box<dyn ...>`) at construction time. On construction, executes the recovery sequence (§5.10) before accepting any RPCs. |
| **`EventLoop`** | Single-threaded async loop that processes protocol state transitions without blocking on I/O. The loop drains an inbound message queue (`tokio::sync::mpsc` — fed by the `ReceiverTask`, §4.4) and dispatches to the appropriate handler. Uses the injected `Clock` directly for timer management (election timeouts, check-quorum deadlines, fetch intervals). **Processing order per message:** (1) The handler mutates `ConsensusState` (e.g., updating follower progress, recalculating HW on a Fetch request, or recording appended entries); (2) If the state change triggers application-visible effects — HW advancement, leadership change — the loop invokes callbacks in a fixed order: `StateMachine::apply` (one call per committed command entry), then `Listener::handle_commit` (one batch of committed `AppRecord` values), then `DeferredCompletionQueue::complete` (resolves client futures for committed offsets); (3) The handler collects `IoAction` values into an `IoActionBatch` (e.g., `SendRpc` for the Fetch response, `AppendLog` for newly staged entries); (4) The loop hands the batch to the `IoStage`, which executes storage and network-send operations concurrently; (5) The loop records I/O results (e.g., advancing the durable offset after `AppendLog` completes). Callbacks in step 2 are synchronous, in-process function calls — not external I/O — and always observe the fully updated protocol state before any IoAction is dispatched. Callbacks must be lightweight and non-blocking; applications that need heavy processing should hand off work to their own async tasks. This prevents slow `fsync` calls from delaying Fetch processing and triggering spurious elections. |
| **`IoStage`** | Executes `IoAction` batches produced by the `EventLoop`. Each action is one of: `PersistQuorumState(QuorumState)`, `AppendLog(Vec<LogEntry>)`, `TruncateSuffix(u64)`, `TruncatePrefix(u64)`, `SendRpc(NodeId, RpcEnvelope)`, `SaveSnapshot(Snapshot)`. The `IoStage` calls the injected I/O trait objects (`LogStore`, `TransportSender`, `QuorumStateStore`, `SnapshotIO`) concurrently via `tokio::join!` or `FuturesUnordered`. Storage operations complete with `fsync` before the loop processes the next message that depends on them. **Application callbacks** (`StateMachine::apply`, `Listener::handle_commit`, `Listener::handle_leader_change`) are NOT dispatched by the `IoStage` — they are invoked directly by the `EventLoop` during message processing, immediately after a state change triggers them (e.g., HW advancement during Fetch handling). This ensures callbacks execute synchronously within the event loop's single-threaded context and always see consistent, up-to-date protocol state. The event loop produces the `IoAction` batch *after* callbacks have been invoked, so the Fetch response sent via `IoStage` reflects the same HW that callbacks observed. **Note:** The `IoStage` does NOT call `TransportReceiver` or `Clock` — those are used by the `ReceiverTask` (§4.4) and `EventLoop` respectively. |
| **`BatchAccumulator`** | Stages incoming `propose()` calls into a batch buffer. On each event-loop tick (or when the batch is full), the accumulated entries are drained into a single `AppendLog` I/O action. This amortises `fsync` cost across multiple proposals (group commit). Analogous to KRaft's `BatchAccumulator`. |
| **`DeferredCompletionQueue`** | Parks `tokio::sync::oneshot` senders keyed by log offset. When the high watermark advances, the queue completes all futures whose offset is now **< HW** (strictly less than — see §3.1 canonical HW definition). Analogous to KRaft's `DeferredEventQueue` / purgatory. |
| **`ConsensusState`** | The core state: current `term`, `voted_for`, node `role` (Follower / Candidate / Leader / Unattached), the in-memory log index, `high_watermark`, `log_start_offset`, the voter set, and per-follower replication progress (leader only). The `Unattached` role is the initial state before bootstrap or recovery completes. |
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

Abstracts network communication behind two split traits so the core never
touches sockets directly. `TransportSender` handles outbound RPCs (called
by the `IoStage` via `SendRpc` actions). `TransportReceiver` handles
inbound RPCs (called by the `ReceiverTask`, §4.4, which feeds the
`EventLoop`'s mpsc channel). The split avoids ownership conflicts — the
sender must be `Sync` for concurrent sends, while the receiver takes
`&mut self` for exclusive read access. The proposed production
implementation uses `tokio` TCP; the proposed test implementation uses
in-process channels.

| Sub-component | Responsibility |
|---------------|----------------|
| **`TransportSender` trait** | Defines `async fn send(&self, target: NodeId, message: RpcEnvelope) → Result<()>`. Takes `&self` (shared reference) because the `IoStage` may send to multiple peers concurrently. Requires `Send + Sync + 'static`. |
| **`TransportReceiver` trait** | Defines `async fn recv(&mut self) → Result<RpcEnvelope>`. Takes `&mut self` (exclusive access) because only the `ReceiverTask` reads from the network. Requires `Send + 'static`. |
| **`TcpTransport`** | Production transport using `tokio::net::TcpStream`. Implements both `TransportSender` and `TransportReceiver`. On construction, `RaftNode` calls `split()` to obtain separate sender and receiver handles. Connections are pooled per peer. Messages are length-prefixed, serialised with `serde` + `bincode`. Each connection is multiplexed by RPC type. |
| **`ChannelTransport`** | In-process transport for integration tests. Uses `tokio::sync::mpsc` channels. Provides `split()` for sender/receiver separation. Supports fault injection: message delay, drop, reorder, partition. |
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

### 3.1 Canonical Offset and Commit Semantics

These definitions are the single source of truth for commit-related
semantics throughout this document and sibling planning documents.

| Term | Definition |
|------|------------|
| **`fetch_offset`** | The next offset a follower wants to read, equal to the follower's `log_end_offset`. A follower reporting `fetch_offset = N` has replicated entries `[0, N)` — offsets 0 through N−1 inclusive. |
| **High watermark (HW)** | An **exclusive upper bound** on committed offsets. An entry at offset O is committed if and only if `O < HW`. Equivalently, `HW − 1` is the last committed offset. HW is never persisted (see §5.10). |
| **HW advancement rule** | The leader collects one value per voter: `fetch_offset` for each follower, `log_end_offset` for itself. Sort these V values in **descending** order. The new HW candidate is the value at index `⌊V/2⌋` (0-indexed). HW advances to `max(current_HW, candidate)` — it never decreases. At least `⌊V/2⌋ + 1` voters (a majority) have `fetch_offset ≥ HW`, meaning all entries in `[0, HW)` are replicated on a majority. |
| **Commit test** | Entry at offset N is committed ⟺ `N < HW`. The `DeferredCompletionQueue` fires a client future when the entry's offset satisfies this test. |
| **Two-round visibility** | A follower needs **two** Fetch rounds to observe a newly committed entry: round 1 delivers the entry (follower's `fetch_offset` has not yet been reported to the leader); round 2 carries the follower's updated `fetch_offset`, which the leader uses to advance HW, returning the new HW in the response. |

*Examples (all use exclusive semantics):*

- **V=3, values=[10, 8, 5]:** Sorted desc → [10, 8, 5]. Index ⌊3/2⌋=1 → HW=8.
  Two voters have offset ≥ 8, so entries [0, 8) (offsets 0–7) are committed.
- **V=5, values=[10, 8, 7, 5, 3]:** Index ⌊5/2⌋=2 → HW=7.
  Three voters have offset ≥ 7, so entries [0, 7) committed.
- **Commit test:** HW=6 → entry at offset 5 is committed (5 < 6 ✓);
  entry at offset 6 is NOT committed (6 < 6 ✗).

### 3.2 Core Entities

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
    entry_type: EntryType           // Command | LeaderChangeMessage | VotersRecord
    payload: Bytes                  // serialised command or control record
}
```

`EntryType` variants — the log contains two classes of entries:

- **`Command`** — application-level state machine command (wraps an
  `AppRecord`). The only entry type delivered to `StateMachine::apply`.
  These are **application records**.

The following two are **consensus control records**, owned entirely by
xraft. They travel through the log like any entry (replicated, committed,
snapshotted) but are never exposed to the application's `StateMachine`:

- **`LeaderChangeMessage`** — control record committed by a new leader to
  establish commit state for the new term. When committed, the event loop
  updates the leader-epoch checkpoint internally. Never reaches `apply`.
- **`VotersRecord`** — control record encoding a membership change (add,
  remove, or update a voter). When committed, the event loop updates the
  in-memory voter set internally. Never reaches `apply`.

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
    role: Role                      // Unattached | Follower | Candidate | Leader
    leader_id: Option<NodeId>       // known leader (if any)

    // Log boundaries
    log_start_offset: u64           // first offset still in the log (after compaction)
    log_end_offset: u64             // next offset to be appended
    high_watermark: u64             // exclusive upper bound of committed offsets;
                                    // entries with offset < HW are committed.
                                    // HW − 1 is the last committed offset.
                                    // (see §3.1 for canonical definition)

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
    fetch_offset: u64               // from the latest FetchRequest: the next offset
                                    // this follower wants to read (= follower's
                                    // log_end_offset). The follower has replicated
                                    // entries [0, fetch_offset). Used directly in
                                    // HW calculation.
    last_fetch_timestamp: Instant   // when this follower last sent a Fetch
    is_voter: bool                  // whether this follower counts for quorum
}
```

#### `Snapshot` (composite: consensus metadata + application payload)

```
SnapshotMetadata {
    last_included_offset: u64       // last log entry included in snapshot
    last_included_term: Term        // term of that entry
    voters: Vec<VoterInfo>          // voter set at snapshot time
    leader_epoch: Term              // leader epoch at snapshot time
}

AppSnapshot {
    data: Vec<u8>                   // serialised application state (opaque to xraft)
}

Snapshot {
    metadata: SnapshotMetadata      // owned by xraft — consensus state
    app_snapshot: AppSnapshot       // owned by the application — state machine state
}
```

The split ensures xraft can read consensus metadata (voter set, offsets)
without deserialising the application payload. On recovery, xraft restores
its own metadata first, then calls `StateMachine::restore(app_snapshot)`.

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

#### `IoAction` (event loop → I/O stage)

```
enum IoAction {
    PersistQuorumState(QuorumState)     // fsync quorum-state file
    AppendLog(Vec<LogEntry>)            // append + fsync log segment
    TruncateSuffix(u64)                 // truncate log from offset (divergence)
    TruncatePrefix(u64)                 // truncate log up to offset (compaction)
    SendRpc(NodeId, RpcEnvelope)        // send message to peer
    SaveSnapshot(Snapshot)              // write snapshot atomically
}
// NOTE: Application callbacks (StateMachine::apply, Listener::handle_commit,
// Listener::handle_leader_change) are NOT IoAction variants. They are
// synchronous, in-process calls invoked directly by the EventLoop during
// message processing — before the IoAction batch is produced. See §4.1.
```

The event loop processes each inbound message (RPC, proposal, timer tick)
in a strict sequence: (1) mutate `ConsensusState`; (2) invoke application
callbacks synchronously if needed (see §4.1 three-phase commit notification);
(3) collect zero or more `IoAction` values into an `IoActionBatch`. After
callbacks and IoAction collection complete, the batch is handed to the
`IoStage` for concurrent execution. The event loop `await`s the `IoStage`
result before processing the next message, ensuring that all external I/O
for a given message completes before state advances. This staging model
keeps the consensus state machine and application callbacks purely
synchronous while allowing external I/O to be parallelised (e.g., `fsync`
the log and send RPCs concurrently).

#### `AppRecord` and `AppSnapshot` (application-owned types)

```
AppRecord {
    data: Bytes                         // opaque serialised command
}

AppSnapshot {
    data: Vec<u8>                       // opaque serialised state machine state
}
```

These are newtype wrappers. xraft never interprets their contents; it only
stores, replicates, and delivers them to the application's `StateMachine`.

### 3.3 RPC Messages

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
    fetch_offset: u64               // next offset the follower wants to read
                                    // (= follower's log_end_offset; follower
                                    // has entries [0, fetch_offset))
    last_fetched_epoch: Term        // epoch of the follower's last log entry
    max_bytes: u32                  // maximum response size
}

FetchResponse {
    leader_id: NodeId
    leader_epoch: Term
    high_watermark: u64             // exclusive upper bound: entries with
                                    // offset < HW are committed
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

### 3.4 Segment File Layout

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

xraft defines three categories of traits with different dispatch models.
Each trait has a single, unambiguous caller — there is no overlap.

| Category | Traits | Dispatch | Bounds | Caller |
|----------|--------|----------|--------|--------|
| **Application** (synchronous) | `StateMachine`, `Listener` | Static (generic type parameters on `RaftNode<S, L>`, monomorphised) | `Send + 'static` | `EventLoop` — invoked synchronously during message processing, before `IoAction` batch is produced. Must be lightweight and non-blocking. |
| **Storage / Network-Send I/O** (asynchronous) | `LogStore`, `SnapshotIO`, `QuorumStateStore`, `TransportSender` | Dynamic (injected as `Box<dyn ...>` trait objects) | `Send + Sync + 'static`, `#[async_trait]` | `IoStage` — invoked concurrently when executing `IoAction` batches (`AppendLog`, `SaveSnapshot`, `PersistQuorumState`, `TruncateSuffix`, `TruncatePrefix`, `SendRpc`) |
| **Runtime** (asynchronous) | `TransportReceiver`, `Clock` | Dynamic (injected as `Box<dyn ...>` trait objects) | `Send + 'static`, `#[async_trait]` | `TransportReceiver`: called by `ReceiverTask` (§4.4) which feeds the `EventLoop`'s mpsc channel. `Clock`: used directly by the `EventLoop` for timer management (election timeouts, check-quorum deadlines). Neither is mediated by `IoAction`. |

This separation ensures that application callbacks always see consistent,
fully-updated protocol state (they run inside the event loop before any I/O
is dispatched), while external I/O is parallelised by the `IoStage`.

#### `StateMachine` (application → core)

The `StateMachine` trait receives **only application records** (`AppRecord`).
Consensus control records (`LeaderChangeMessage`, `VotersRecord`) are
handled internally by xraft and never reach `apply`. This boundary is
enforced by the `EventLoop`: when the high watermark advances, the loop
iterates over newly committed `LogEntry` values, applies control records
internally (e.g., updating the voter set from a `VotersRecord`, recording
the leader-epoch from a `LeaderChangeMessage`), and calls
`StateMachine::apply` only for entries whose `entry_type` is `Command`.

**Three-phase commit notification (fixed ordering).** When HW advances
during message processing (e.g., when a Fetch request reveals that a
majority has replicated), the `EventLoop` executes these steps in order
before producing any `IoAction`:

1. **`StateMachine::apply`** — called once per newly committed command entry.
   Mutates application state. Control records are filtered and processed
   internally (e.g., updating the voter set).
2. **`Listener::handle_commit`** — called once with the full batch of newly
   committed `AppRecord` values. Used for external notification (metrics,
   indexing, replication to external systems). Receives only application
   records; control records are filtered.
3. **`DeferredCompletionQueue::complete`** — resolves the `oneshot` future
   for every committed entry whose offset is now `< HW`.

All three steps are **synchronous, in-process function calls** within the
event loop's single-threaded task — they are not external I/O and are not
mediated by `IoAction` / `IoStage`. External I/O (sending the Fetch
response, appending log entries) is dispatched via `IoAction` *after*
callbacks have completed.

Snapshots are split into two parts:
- **Consensus metadata** (`SnapshotMetadata` — term, offset, voter set,
  log bounds) owned by xraft.
- **Application payload** (`AppSnapshot` — opaque bytes) owned by the
  application via `StateMachine::snapshot()` / `restore()`.

```rust
pub trait StateMachine: Send + 'static {
    /// Apply a committed application record to the state machine.
    /// Control records (NoOp, VotersRecord) are never passed here.
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>;

    /// Take a snapshot of the current application state.
    fn snapshot(&self) -> Result<AppSnapshot>;

    /// Restore application state from a snapshot.
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>;
}
```

`AppRecord` is a newtype around `Bytes` — the application's serialised
command. `AppSnapshot` is likewise opaque bytes. xraft never interprets
either; it only stores, replicates, and delivers them.

#### `Listener` (core → application callbacks)

```rust
pub trait Listener: Send + 'static {
    /// Called when a batch of application records is committed (HW advanced).
    /// Only application records appear here; control records are filtered.
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]);

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

#### `TransportSender` (core → network, outbound RPCs)

```rust
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    /// Send a message to a specific node. Called by IoStage via SendRpc action.
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}
```

`TransportSender` requires `Sync` because the `IoStage` may send to
multiple peers concurrently from the same trait object.

#### `TransportReceiver` (network → core, inbound RPCs)

```rust
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    /// Receive the next inbound message. Called exclusively by ReceiverTask (§4.4).
    async fn recv(&mut self) -> Result<RpcEnvelope>;
}
```

`TransportReceiver` takes `&mut self` because only one task (`ReceiverTask`)
reads from the network. It does NOT require `Sync`.

Production: `TcpTransport` implements both traits and exposes a `split()`
method that returns `(Box<dyn TransportSender>, Box<dyn TransportReceiver>)`.
`RaftNode` calls `split()` at construction time and passes the sender half
to the `IoStage` and the receiver half to the `ReceiverTask`.

Test: `ChannelTransport` provides the same `split()` interface, returning
in-process channel halves with optional fault injection.

#### `Clock` (core ↔ time)

```rust
#[async_trait]
pub trait Clock: Send + 'static {
    /// Current instant.
    fn now(&self) -> Instant;

    /// Sleep until the given deadline.
    async fn sleep_until(&self, deadline: Instant);

    /// Generate a random election timeout in [min, max].
    fn random_election_timeout(&self) -> Duration;
}
```

`Clock` is used directly by the `EventLoop` — not mediated by `IoAction`.
It does not require `Sync` because only the single-threaded event loop
accesses it.

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
                     │ │(AppRec   │            │(AppRec    │          │
                     │ │ only)    │            │  only)    │          │
                     │ └─────┬────┘            └─────┬─────┘          │
                     └───────┼───────────────────────┼────────────────┘
                             │                       │
               propose()    │    apply(AppRecord)    │ handle_commit
               read()       │    snapshot/restore    │ handle_leader_change
                             │                       │ handle_load_snapshot
                     ┌───────▼───────────────────────▼────────────────┐
                     │                 RaftNode                        │
                     │           ┌──────────────┐                     │
                     │           │  EventLoop   │                     │
                     │           │ (msg queue → │                     │
                     │           │  IoAction    │                     │
                     │           │  batch)      │                     │
                     │           └──────┬───────┘                     │
                     │    ┌─────────────┼────────────────┐            │
                     │    ▼             ▼                ▼            │
                     │ Election     Replication     Membership        │
                     │ Manager     Manager         Manager            │
                     │    │             │                │            │
                     │    └──────┬──────┘────────────────┘            │
                     │           ▼                                    │
                     │   ┌──────────────┐  ┌─────────────────────┐   │
                     │   │BatchAccum.   │  │ DeferredCompletion  │   │
                     │   │(stage writes)│  │ Queue (park futures)│   │
                     │   └──────┬───────┘  └─────────────────────┘   │
                     │          ▼                                     │
                     │   ┌──────────────┐                             │
                     │   │  IoStage     │ ◄── executes IoAction      │
                     │   │  (concurrent │     batch after each       │
                     │   │   I/O exec)  │     event-loop tick        │
                     │   └──────┬───────┘                             │
                     └──────────┼──────────────────────────────────────┘
                                │
          ┌─────────────────────┼───────────────────────────────────┐
          ▼             ▼       ▼             ▼                     ▼
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
       │          │ log_end=6  │             │                   │
       │          └─────┬──────┘             │                   │
       │                │                    │                   │
       │                │  ◄── FetchRequest ─│                   │
       │                │      (fetch_off=5, │                   │
       │                │       epoch=T)     │                   │
       │                │                    │                   │
       │          ┌─────┴──────┐             │                   │
       │          │ Update B's │             │                   │
       │          │ progress:  │             │                   │
       │          │ B.fetch=5  │             │                   │
       │          │ (B has 0-4)│             │                   │
       │          │ HW calc:   │             │                   │
       │          │ [A=6,B=5,  │             │                   │
       │          │  C=5]      │             │                   │
       │          │ sorted →   │             │                   │
       │          │ [6,5,5]    │             │                   │
       │          │ idx ⌊3/2⌋  │             │                   │
       │          │ =1 → 5     │             │                   │
       │          │ HW stays 5 │             │                   │
       │          │ (was 5     │             │                   │
       │          │  already)  │             │                   │
       │          └─────┬──────┘             │                   │
       │                │                    │                   │
       │                │── FetchResponse ──►│                   │
       │                │   entries=[off=5], │                   │
       │                │   HW=5 (off 5 not  │                   │
       │                │   committed: 5≮5)  │                   │
       │                │                    │                   │
       │                │                    │  ┌──────────┐     │
       │                │                    │  │Append 5  │     │
       │                │                    │  │fsync     │     │
       │                │                    │  │B.log_end │     │
       │                │                    │  │  =6      │     │
       │                │                    │  └──────────┘     │
       │                │                    │                   │
       │                │  ◄── FetchRequest ─┼───────────────────│
       │                │      (fetch_off=5) │  (C has 0-4)      │
       │                │                    │                   │
       │                │── FetchResponse ───┼──────────────────►│
       │                │   entries=[off=5], │                   │
       │                │   HW=5 (unchanged) │                   │
       │                │                    │                   │
       │                │                    │  ◄── Round 2 ───  │
       │                │  ◄── FetchRequest ─│                   │
       │                │      (fetch_off=6) │                   │
       │                │                    │                   │
       │          ┌─────┴──────┐             │                   │
       │          │ Update B's │             │                   │
       │          │ progress:  │             │                   │
       │          │ B.fetch=6  │             │                   │
       │          │ (B has 0-5)│             │                   │
       │          │ HW calc:   │             │                   │
       │          │ [A=6,B=6,  │             │                   │
       │          │  C=5]      │             │                   │
       │          │ sorted →   │             │                   │
       │          │ [6,6,5]    │             │                   │
       │          │ idx 1 → 6  │             │                   │
       │          │ HW ← 6    │             │                   │
       │          │ ────────── │             │                   │
       │          │ off 5 now  │             │                   │
       │          │ committed  │             │                   │
       │          │ (5 < 6) ✓  │             │                   │
       │          │ SM.apply(5)│             │                   │
       │          │ Complete   │             │                   │
       │          │ client fut.│             │                   │
       │          └─────┬──────┘             │                   │
       │                │                    │                   │
       │                │── FetchResponse ──►│                   │
       │                │   entries=[],      │                   │
       │                │   HW=6 ◄── commit  │                   │
       │                │   visible to B     │                   │
       │                │                    │                   │
       │◄── Ok(result) ─│                    │                   │
       │   (committed)  │                    │  B.apply(off=5)   │
       │                │                    │                   │
```

**Fetch-offset semantics (critical definition).** A follower's
`fetch_offset` in a `FetchRequest` is the **next offset the follower wants
to read** — equivalently, the follower's `log_end_offset`. A follower with
`fetch_offset = N` has replicated entries `[0, N)` (offsets 0 through N−1
inclusive). The leader records this value in `FollowerProgress.fetch_offset`
and uses it directly in the HW calculation. The leader's own contribution
to the HW calculation is its `log_end_offset`.

**High-watermark semantics.** HW is an **exclusive upper bound** — entry at
offset O is committed when `O < HW`. Equivalently, `HW − 1` is the last
committed offset. HW is never persisted to disk (see §5.10 Crash Recovery).

**High-watermark advancement rule (quorum math).** The leader maintains
`FollowerProgress` for each voter. On each incoming Fetch request, the
leader (1) updates the follower's `fetch_offset`, (2) recalculates HW,
(3) includes the new HW in the FetchResponse. To compute the new HW,
the leader collects `fetch_offset` for every voter (including itself —
using its own `log_end_offset`) and sorts them in **descending** order.
The new HW is the value at index `⌊V/2⌋` (0-indexed), where `V` is the
total number of voters. This is the highest offset at or above which at
least a **majority** (`⌊V/2⌋ + 1`) of voters have replicated. HW can
only advance forward — it never decreases.

*Example (V=3, fetch_offsets [10, 8, 5]):* Sorted descending: [10, 8, 5].
Index ⌊3/2⌋ = 1 → HW = 8. Two voters have fetch_offset ≥ 8, meaning
both have entries [0, 8). Majority reached → entries 0–7 committed.

*Example (V=5, fetch_offsets [10, 8, 7, 5, 3]):* Sorted descending:
[10, 8, 7, 5, 3]. Index ⌊5/2⌋ = 2 → HW = 7. Three voters have
fetch_offset ≥ 7 → entries 0–6 committed.

*Example (V=4, fetch_offsets [10, 8, 5, 3]):* Sorted descending:
[10, 8, 5, 3]. Index ⌊4/2⌋ = 2 → HW = 5. Three voters have
fetch_offset ≥ 5 → entries 0–4 committed.

Only voters count; observers do not contribute to quorum.

**Two-round commit visibility.** A follower fetches new entries in round 1.
At that point its `fetch_offset` has not yet increased (the leader recorded
the OLD value when the Fetch arrived). In round 2, the follower sends a new
Fetch with its updated `fetch_offset`, which triggers the leader to
recalculate HW and include the advanced value in the response. The follower
then sees the new HW and can apply newly committed entries to the state
machine. This is inherent to the pull-based model — the Fetch that delivers
entries cannot also deliver the HW that commits them, because the leader
has not yet counted the follower's replication of those entries.

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

### 5.6 Dynamic Membership Change (Remove Voter)

Removing an existing voter from the cluster. Covers both the case where a
follower is removed and the special case where the leader removes itself.

```
    Admin         Leader (N1)       Follower N2       Follower N3
      │              │                 │                 │
      │─ RemoveVoter►│                 │                 │
      │  (node=N3)   │                 │                 │
      │              │                 │                 │
      │        ┌─────┴──────┐          │                 │
      │        │ Validate:  │          │                 │
      │        │ 1. Am I    │          │                 │
      │        │    leader? │          │                 │
      │        │ 2. No      │          │                 │
      │        │    pending │          │                 │
      │        │    change? │          │                 │
      │        │ 3. N3 in   │          │                 │
      │        │    voter   │          │                 │
      │        │    set?    │          │                 │
      │        │ 4. Append  │          │                 │
      │        │    Voters- │          │                 │
      │        │    Record  │          │                 │
      │        │    voters= │          │                 │
      │        │    [N1,N2] │          │                 │
      │        └─────┬──────┘          │                 │
      │              │                 │                 │
      │              │  ◄── Fetch ─────│                 │
      │              │── FetchResp ───►│                 │
      │              │  (includes      │                 │
      │              │   VotersRecord  │  ◄── Fetch ─────│
      │              │   [N1,N2])      │                 │
      │              │                 │                 │
      │              │── FetchResp ────┼────────────────►│
      │              │  (includes      │                 │
      │              │   VotersRecord  │                 │
      │              │   [N1,N2])      │                 │
      │              │                 │                 │
      │        ┌─────┴──────┐          │                 │
      │        │ Majority   │          │                 │
      │        │ of NEW     │          │                 │
      │        │ config     │          │                 │
      │        │ [N1,N2]    │          │                 │
      │        │ fetched    │          │                 │
      │        │ VotersRec. │          │                 │
      │        │ HW adv.    │          │                 │
      │        │ N3 is no   │          │                 │
      │        │ longer a   │          │                 │
      │        │ voter.     │          │                 │
      │        └─────┬──────┘          │                 │
      │              │                 │                 │
      │◄── Ok ───────│                 │                 │
      │              │                 │                 │
```

**Quorum calculation during RemoveVoter.** The `VotersRecord` is committed
using the **new** voter set for quorum purposes. In the example above, once
the VotersRecord `[N1, N2]` is appended, HW advancement requires a majority
of `{N1, N2}` (i.e., both nodes). N3's fetch progress is no longer counted.
This ensures the new configuration is durable on a majority of the new
membership before taking effect.

**Leader self-removal.** When the leader receives `RemoveVoter` for itself:

```
    Admin         Leader (N1)       Follower N2       Follower N3
      │              │                 │                 │
      │─ RemoveVoter►│                 │                 │
      │  (node=N1)   │                 │                 │
      │              │                 │                 │
      │        ┌─────┴──────┐          │                 │
      │        │ Append     │          │                 │
      │        │ VotersRec  │          │                 │
      │        │ [N2,N3]    │          │                 │
      │        │ Continue   │          │                 │
      │        │ as leader  │          │                 │
      │        │ until      │          │                 │
      │        │ committed  │          │                 │
      │        └─────┬──────┘          │                 │
      │              │                 │                 │
      │              │  ◄── Fetch ─────│                 │
      │              │── FetchResp ───►│  ◄── Fetch ─────│
      │              │── FetchResp ────┼────────────────►│
      │              │                 │                 │
      │        ┌─────┴──────┐          │                 │
      │        │ VotersRec  │          │                 │
      │        │ committed  │          │                 │
      │        │ (majority  │          │                 │
      │        │  of [N2,N3]│          │                 │
      │        │  fetched)  │          │                 │
      │        │            │          │                 │
      │        │ N1 steps   │          │                 │
      │        │ down to    │          │                 │
      │        │ Unattached │          │                 │
      │        └─────┬──────┘          │                 │
      │              │                 │                 │
      │◄── Ok ───────│                 │                 │
      │              │                 │                 │
      │              │           ┌─────┴──────┐          │
      │              │           │ N2 or N3   │          │
      │              │           │ election   │          │
      │              │           │ timeout →  │          │
      │              │           │ new leader │          │
      │              │           └─────┬──────┘          │
      │              │                 │                 │
```

**Leader self-removal rules:**
1. The leader appends the `VotersRecord` that excludes itself and continues
   serving as leader (processing Fetch requests, advancing HW) until the
   `VotersRecord` is committed.
2. Commitment requires a majority of the **new** voter set (which does not
   include the departing leader). This guarantees the new configuration is
   durable on the surviving members.
3. Once committed, the leader steps down to `Unattached` and stops accepting
   proposals. It does not transition to `Follower` because it is no longer
   a member of the voter set.
4. The remaining voters' election timers fire, and a new leader is elected
   from the new configuration.

**Removed node behaviour.** After N3 learns of its removal (by fetching
and applying the `VotersRecord` that excludes it), N3 transitions to
`Unattached` and stops participating in elections. If N3 has not yet
learned of its removal and attempts an election, the Pre-Vote protocol
and the remaining voters' knowledge of the current voter set cause them
to reject N3's Vote requests — preventing cluster disruption.

### 5.7 Check Quorum (Leader Liveness)

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

### 5.8 Client Proposal (Full Path — with I/O Staging)

End-to-end flow from client command to committed state machine application.
The `EventLoop` mutates state and invokes callbacks synchronously; the
`IoStage` executes external I/O (`LogStore::append`, `Transport::send`)
via injected trait objects. No raw I/O occurs inside `xraft-core`.

```
    Client          RaftNode       EventLoop       BatchAccum.   IoStage        LogStore
      │                │               │               │            │              │
      │── propose(cmd)►│               │               │            │              │
      │                │               │               │            │              │
      │                │── Propose ───►│               │            │              │
      │                │  (mpsc chan.) │               │            │              │
      │                │               │               │            │              │
      │                │         ┌─────┴──────┐        │            │              │
      │                │         │ Am I       │        │            │              │
      │                │         │ leader?    │        │            │              │
      │                │         │ YES        │        │            │              │
      │                │         └─────┬──────┘        │            │              │
      │                │               │               │            │              │
      │                │               │── stage(cmd)─►│            │              │
      │                │               │               │            │              │
      │                │               │  Park oneshot in            │              │
      │                │               │  DeferredCompletionQueue    │              │
      │                │               │  (keyed by offset N)       │              │
      │                │               │               │            │              │
      │                │         ┌─────┴──────┐        │            │              │
      │                │         │ Tick: drain │        │            │              │
      │                │         │ batch       │        │            │              │
      │                │         └─────┬──────┘        │            │              │
      │                │               │               │            │              │
      │                │               │◄─ entries ────│            │              │
      │                │               │  (drained)    │            │              │
      │                │               │               │            │              │
      │                │               │── IoAction::  │            │              │
      │                │               │  AppendLog ───┼───────────►│              │
      │                │               │               │            │── .append()─►│
      │                │               │               │            │  (await)     │
      │                │               │               │            │◄─ Ok(fsync)─│
      │                │               │◄─ IoResult ───┼────────────│              │
      │                │               │  (durable)    │            │              │
      │                │               │               │            │              │
      │                │       ... followers fetch entry N ...      │              │
      │                │       ... Fetch triggers HW advance ...    │              │
      │                │               │               │            │              │
      │                │         ┌─────┴──────┐        │            │              │
      │                │         │ HW ≥ N+1   │        │            │              │
      │                │         │ (N < HW ✓) │        │            │              │
      │                │         │ Three-phase │        │            │              │
      │                │         │ commit:     │        │            │              │
      │                │         │ 1. Filter:  │        │            │              │
      │                │         │  Control →  │        │            │              │
      │                │         │  internal   │        │            │              │
      │                │         │  Command →  │        │            │              │
      │                │         │  SM.apply   │        │            │              │
      │                │         │ 2. Listener │        │            │              │
      │                │         │  .handle_   │        │            │              │
      │                │         │  commit     │        │            │              │
      │                │         │ 3. Deferred │        │            │              │
      │                │         │  Complete   │        │            │              │
      │                │         │  Queue:     │        │            │              │
      │                │         │  resolve    │        │            │              │
      │                │         │  oneshot    │        │            │              │
      │                │         │ (all sync,  │        │            │              │
      │                │         │  in-process)│        │            │              │
      │                │         │             │        │            │              │
      │                │         │ Then produce│        │            │              │
      │                │         │ IoAction::  │        │            │              │
      │                │         │ SendRpc for │        │            │              │
      │                │         │ FetchResp   │        │            │              │
      │                │         └─────┬──────┘        │            │              │
      │                │               │               │            │              │
      │                │◄── Result ────│               │            │              │
      │                │               │               │            │              │
      │◄── Ok(result) ─│               │               │            │              │
      │                │               │               │            │              │
```

If the node is not the leader, `propose()` returns
`Err(NotLeader { leader_id })` so the client can redirect.

### 5.9 Cluster Bootstrap

A fresh cluster with no prior state. Each node is configured with a static
initial voter set and a shared `cluster_id`. All nodes start in the
`Unattached` role and transition to `Follower` once bootstrap completes.

```
    Node N1 (Unattached)         Node N2 (Unattached)       Node N3 (Unattached)
         │                            │                          │
   ┌─────┴──────┐              ┌──────┴──────┐           ┌──────┴──────┐
   │ Startup:   │              │ Startup:    │           │ Startup:    │
   │ 1. No      │              │ Same as N1  │           │ Same as N1  │
   │  quorum-st │              └──────┬──────┘           └──────┬──────┘
   │  file →    │                     │                         │
   │  term=0    │                     │                         │
   │ 2. No      │                     │                         │
   │  snapshot  │                     │                         │
   │ 3. Empty   │                     │                         │
   │  log       │                     │                         │
   │ 4. Load    │                     │                         │
   │  bootstrap │                     │                         │
   │  voter set │                     │                         │
   │  from      │                     │                         │
   │  config    │                     │                         │
   │ 5. Set     │                     │                         │
   │  role ←    │                     │                         │
   │  Follower  │                     │                         │
   │ 6. Start   │                     │                         │
   │  election  │                     │                         │
   │  timer     │                     │                         │
   └─────┬──────┘                     │                         │
         │                            │                         │
         │  (N1's election timeout expires first)               │
         │                            │                         │
         │── VoteRequest(term=1) ────►│                         │
         │── VoteRequest(term=1) ─────┼────────────────────────►│
         │                            │                         │
         │◄─ VoteResponse(granted) ──│                         │
         │◄─ VoteResponse(granted) ──┼─────────────────────────│
         │                            │                         │
   ┌─────┴──────┐                     │                         │
   │ Become     │                     │                         │
   │ LEADER     │                     │                         │
   │ term=1     │                     │                         │
   │            │                     │                         │
   │ Append:    │                     │                         │
   │ 1. Leader- │                     │                         │
   │  ChangeMes.│                     │                         │
   │  @off=0    │                     │                         │
   │ 2. Voters- │                     │                         │
   │  Record    │                     │                         │
   │  @off=1    │                     │                         │
   │  voters=   │                     │                         │
   │  [N1,N2,N3]│                     │                         │
   └─────┬──────┘                     │                         │
         │                            │                         │
         │  ◄── Fetch(off=0) ────────│                         │
         │── FetchResp(entries 0,1) ─►│                         │
         │                            │                         │
         │  ◄── Fetch(off=0) ─────────┼────────────────────────│
         │── FetchResp(entries 0,1) ──┼───────────────────────►│
         │                            │                         │
   ┌─────┴──────┐                     │                         │
   │ HW ← 1    │                     │                         │
   │ VotersRec  │                     │                         │
   │ committed. │                     │                         │
   │ Cluster is │                     │                         │
   │ fully      │                     │                         │
   │ bootstrap. │                     │                         │
   └─────┬──────┘                     │                         │
         │                            │                         │
```

**Bootstrap invariants:**
- A node with no `quorum-state` file and no log is considered uninitialized.
  It obtains its voter set from the static configuration (not from the log).
- Once the initial `VotersRecord` is committed, the cluster is bootstrapped.
  Subsequent voter-set changes use `AddVoter` / `RemoveVoter` RPCs.
- The `cluster_id` is set once at bootstrap and included in every RPC. Nodes
  reject RPCs with a mismatched `cluster_id`.

### 5.10 Crash Recovery

A node that was previously running crashes (or is stopped) and restarts.
Recovery restores durable state and re-enters the cluster as a follower.
**The high watermark is never persisted**, so the recovering node does not
know which log entries were committed before the crash. It relies on the
leader to provide the authoritative HW via subsequent Fetch responses.

```
    Recovering Node N2           Running Leader N1
         │                            │
   ┌─────┴───────────────────┐        │
   │ Phase 1: Restore        │        │
   │ quorum state            │        │
   │                         │        │
   │ Read quorum-state file: │        │
   │  current_term = 5       │        │
   │  voted_for = N1         │        │
   │  leader_epoch = 4       │        │
   └─────┬───────────────────┘        │
         │                            │
   ┌─────┴───────────────────┐        │
   │ Phase 2: Restore log    │        │
   │ and snapshot             │        │
   │                         │        │
   │ a. Load latest snapshot │        │
   │    (if any):            │        │
   │    last_included_off=80 │        │
   │    last_included_term=3 │        │
   │    voters=[N1,N2,N3]    │        │
   │                         │        │
   │ b. Restore state mach.  │        │
   │    from AppSnapshot     │        │
   │    (SM state is now at  │        │
   │     offset 80)          │        │
   │                         │        │
   │ c. Set HW ← 81         │        │
   │    (snapshot offset + 1;│        │
   │     entries [0,81) are  │        │
   │     known committed     │        │
   │     because they are    │        │
   │     included in the     │        │
   │     snapshot)            │        │
   │                         │        │
   │ d. Scan log segments    │        │
   │    from offset 81.      │        │
   │    Verify CRC per batch.│        │
   │    Truncate at first    │        │
   │    corrupt/partial rec. │        │
   │    Entries found:       │        │
   │    81..95 (valid on     │        │
   │    disk, but committed  │        │
   │    status UNKNOWN)      │        │
   │                         │        │
   │ e. DO NOT apply entries │        │
   │    81..95 to the state  │        │
   │    machine. Their       │        │
   │    committed status is  │        │
   │    unknown — some may   │        │
   │    be uncommitted tail  │        │
   │    entries that will be │        │
   │    truncated on         │        │
   │    divergence.          │        │
   │                         │        │
   │ f. Rebuild leader-epoch │        │
   │    checkpoint from log  │        │
   │    (scan for Leader-    │        │
   │    ChangeMessage entries │        │
   │    to build epoch →     │        │
   │    start_offset map)    │        │
   │                         │        │
   │ g. Process control recs │        │
   │    in log (81..95) for  │        │
   │    internal bookkeeping │        │
   │    only:                │        │
   │    - VotersRecord →     │        │
   │      update voter set   │        │
   │    - LeaderChangeMes. → │        │
   │      update leader-     │        │
   │      epoch checkpoint   │        │
   │    (These are consensus │        │
   │    metadata updates,    │        │
   │    NOT state machine    │        │
   │    applications. Even   │        │
   │    if truncated later,  │        │
   │    the leader's Fetch   │        │
   │    will provide correct │        │
   │    entries.)             │        │
   └─────┬───────────────────┘        │
         │                            │
   ┌─────┴───────────────────┐        │
   │ Phase 3: Resume as      │        │
   │ Follower                │        │
   │                         │        │
   │ Set role ← Follower     │        │
   │ (NEVER resume as leader │        │
   │  regardless of prior    │        │
   │  role; must re-win      │        │
   │  election)              │        │
   │                         │        │
   │ Start election timer    │        │
   │ Begin accepting RPCs    │        │
   └─────┬───────────────────┘        │
         │                            │
         │── FetchRequest ───────────►│
         │   (fetch_offset=96,        │
         │    last_fetched_epoch=5)   │
         │                            │
         │◄─ FetchResponse ──────────│
         │   entries=[96..100],       │
         │   HW=101                   │
         │                            │
   ┌─────┴───────────────────┐        │
   │ Phase 4: Catch up via   │        │
   │ leader's HW             │        │
   │                         │        │
   │ Leader says HW=101.     │        │
   │ Local HW was 81.        │        │
   │ Advance local HW to     │        │
   │ min(101, log_end_offset) │        │
   │ = min(101, 101) = 101.  │        │
   │                         │        │
   │ Apply entries 81..100   │        │
   │ to state machine        │        │
   │ (three-phase commit     │        │
   │  notification, §4.1):   │        │
   │ 1. Command entries →    │        │
   │    SM.apply (one per    │        │
   │    entry)               │        │
   │ 2. Listener.handle_     │        │
   │    commit (batch)       │        │
   │ 3. DeferredCompletion   │        │
   │    Queue (no-op here —  │        │
   │    no pending client    │        │
   │    futures post-crash)  │        │
   │ - Control recs →        │        │
   │   filtered out (§4.1:   │        │
   │   never passed to       │        │
   │   SM.apply; handled     │        │
   │   internally by xraft)  │        │
   │                         │        │
   │ Normal operation.       │        │
   └─────┴───────────────────┘        │
         │                            │
```

**Recovery invariants (crash-recovery HW rules):**

1. **HW is never persisted.** On recovery, HW is initialised to
   `snapshot.last_included_offset + 1` (or `0` if no snapshot exists). This
   represents the exclusive upper bound of offsets known to be committed
   (the snapshot captures only committed state). Entries in the log beyond
   this point have **unknown** committed status.

2. **No state machine replay during recovery.** Log entries between the
   snapshot offset and `log_end_offset` are NOT applied to the
   `StateMachine` during recovery, because their committed status is
   unknown. Some may be uncommitted tail entries from a deposed leader
   that will be truncated when the current leader detects divergence.
   Applying them would put the state machine in an incorrect state with
   no rollback mechanism.

3. **Control records are processed for bookkeeping.** `VotersRecord` and
   `LeaderChangeMessage` entries in the recovered log are scanned to
   rebuild the voter set and leader-epoch checkpoint. These are internal
   consensus metadata — not state machine mutations — and are idempotent.
   If the log is later truncated due to divergence, the leader's correct
   entries will overwrite these values.

4. **Leader provides the authoritative HW.** After resuming as follower,
   the node sends Fetch requests to the leader. The leader's Fetch
   response includes the current HW. The node advances its local HW to
   `min(leader_HW, local_log_end_offset)` and executes the three-phase
   commit notification (§4.1) for all entries between the old HW and the
   new HW: `StateMachine::apply` for command entries, `Listener::handle_commit`
   for the batch, and `DeferredCompletionQueue::complete` (no-op post-crash
   since there are no pending client futures). Control records are always
   filtered from `SM::apply` and handled internally.

5. **Always resume as Follower.** A recovering node never resumes as
   leader regardless of its prior role. It must win a new election.
   `current_term` and `voted_for` are read from the `quorum-state` file
   before any RPC is processed, preventing double-voting.

6. **Log integrity via CRC-32C.** Each batch in the log is checksummed.
   The first corrupt or incomplete batch triggers truncation of that batch
   and all subsequent entries. This is safe because entries beyond HW are
   uncommitted by definition (they were not yet replicated to a majority).

7. **Snapshot fallback.** If the recovering node's `fetch_offset` is below
   the leader's `log_start_offset` (the leader has compacted the needed
   entries), the leader responds with a `SnapshotId` and the node falls
   back to snapshot transfer (§5.4).

---

## 6. Cross-Cutting Concerns

### 6.1 Persistence Guarantees

Every write path that affects correctness calls `fsync` before
acknowledgement. The `EventLoop` produces `IoAction` values for all
external I/O; the `IoStage` executes them via injected trait objects. The
concrete `fsync` implementation lives in `xraft-storage`, not in
`xraft-core`. Application callbacks (`StateMachine::apply`,
`Listener::handle_commit`) are synchronous in-process calls, not external
I/O, and are invoked by the event loop before the `IoAction` batch is
dispatched (see §4.1 three-phase commit notification).

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
- **State machine interface** is generic (monomorphised) per §6:
  `RaftNode<S: StateMachine, L: Listener>`. Both application traits use
  synchronous methods (`fn`, not `async fn`) and are invoked directly by
  the event loop — they are not external I/O. The `StateMachine` trait
  receives only `AppRecord` values; control records (`LeaderChangeMessage`,
  `VotersRecord`) are handled internally by xraft and never reach `apply`.
  This matches §2.1.1: "Control records are owned by xraft and are never
  exposed to the application's `StateMachine::apply`." Snapshots are split:
  `SnapshotMetadata` (consensus) and `AppSnapshot` (application).
  I/O traits (`LogStore`, `Transport`, `QuorumStateStore`, `SnapshotIO`,
  `Clock`) are injected as trait objects (`Box<dyn ...>`) and use
  `#[async_trait]` — they are called exclusively by the `IoStage`, never
  directly by the event loop.
- **Segment-file log storage** per §6.
- **I/O staging** per §4.4.1 — the event loop produces `IoAction` values
  and the `IoStage` executes them via injected trait objects for all
  external I/O (disk, network). The loop never directly awaits external I/O
  inline. Application callbacks (`StateMachine::apply`, `Listener`) are
  synchronous in-process calls invoked by the event loop during message
  processing — they are not external I/O and are not mediated by `IoAction`.
  `BatchAccumulator` stages proposals before draining (group commit);
  `DeferredCompletionQueue` parks client futures. This matches the tech
  spec's description of `BatchAccumulator` and `DeferredEventQueue` patterns.
  **Note:** The tech spec §6 states that "application callbacks are staged
  and executed asynchronously outside the loop." This architecture refines
  that statement: callbacks execute within the event loop's single-threaded
  context (not on a separate task) to ensure they always see consistent
  protocol state. They are "staged" in the sense that they are deferred
  until state is updated — not offloaded to another thread or task.
- **Timing parameters** (150–300 ms election timeout, 50 ms fetch interval)
  per §4.3.
- **Quorum math** — majority is `⌊V/2⌋ + 1`; HW advancement uses
  descending-sorted voter `fetch_offset` values at index `⌊V/2⌋`. HW is
  an exclusive upper bound (entry at offset O is committed when O < HW).
  Consistent with §2.1.1 of the tech spec.
- **Bootstrap & recovery** (§5.9, §5.10) align with tech spec §2.1.7.
  Bootstrap uses static voter set → leader commits `VotersRecord`. Recovery
  reads `quorum-state` → loads snapshot → scans log → resumes as follower
  → learns HW from leader → applies committed entries.
- **Membership changes** — §5.5 (AddVoter) and §5.6 (RemoveVoter) cover
  the full lifecycle including leader self-removal. Consistent with tech
  spec §2.1.3.

### Open Items for Sibling Documents

- `implementation-plan.md`: This architecture defines WHAT to build.
  The implementation plan should sequence the work — likely core election →
  log replication → persistence → snapshot → membership → simulation harness.
  The `IoStage` and `BatchAccumulator` should be implemented in the core
  replication phase (they are not optional add-ons).
- `e2e-scenarios.md`: Sequence flows §5.1–§5.10 here define the "happy
  path" and key failure modes including bootstrap (§5.9), crash recovery
  (§5.10), and remove-voter (§5.6). The E2E document should define concrete
  test scenarios with expected assertions for each flow.
