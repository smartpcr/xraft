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
│  │  RaftNode    │  │  EventLoop   │  │  NodeState                │  │
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
The event loop processes protocol messages, mutates `NodeState`, and
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
mutates in-memory `NodeState`, invokes synchronous application
callbacks (`StateMachine`, `Listener`), manages timers via the injected
`Clock`, and emits `IoAction` batches.

| Sub-component | Responsibility |
|---------------|----------------|
| **`RaftNode`** | Public API surface. Exposes `propose()`, `read()` (§5.11), `bootstrap()`, and lifecycle methods. `propose()` appends a command to the log and returns a future resolved on commit. `read()` clones the latest `ConsensusState` from a `tokio::sync::watch` channel (§5.11) — a local, non-linearizable snapshot of selected protocol metadata fields (term, role, leader, HW, voter set, log_end_offset, node_id); callable on any node. Spawns the `EventLoop` task (which **owns** the `IoStage`), the `ReceiverTask`, and retains a `propose_tx: mpsc::Sender` for submitting proposals plus a `watch::Receiver<ConsensusState>` for `read()`. Coordinates startup, shutdown, and crash recovery. Generic over two application-provided types: `S: StateMachine` and `L: Listener` (both monomorphised at compile time for zero-cost dispatch). I/O and runtime traits (`LogStore`, `TransportSender`, `TransportReceiver`, `QuorumStateStore`, `SnapshotIO`, `Clock`) are injected as `Box<dyn ...>` trait objects at construction time. Storage and network-send trait objects are moved into the `IoStage` (which is then moved into the event loop task); `TransportReceiver` is moved into the `ReceiverTask`; `Clock` is moved into the event loop task. No `Arc` wrapping is needed because each trait object has a single owner and concurrent access within an I/O batch uses shared `&self` borrows (safe because all I/O traits require `Sync`). On construction, executes the recovery sequence (§5.10) before accepting any RPCs. |
| **`EventLoop`** | Single-threaded async loop that processes protocol state transitions without blocking on I/O. The event loop task **owns** the `IoStage` (moved in at startup) and holds `&self` access to it for executing I/O batches. The loop drains an inbound message queue (`tokio::sync::mpsc` — fed by the `ReceiverTask`, §4.4, and by `propose()` calls) and dispatches to the appropriate handler. Uses the injected `Clock` directly for timer management (election timeouts, check-quorum deadlines, fetch intervals). **Processing order per message:** (1) The handler mutates `NodeState` (e.g., updating follower progress, recalculating HW on a Fetch request, or recording appended entries); (2) If the state change triggers application-visible effects — HW advancement, leadership change — the loop invokes callbacks in a fixed order: `StateMachine::apply` (one call per committed command entry), then `Listener::handle_commit` (one batch of committed `AppRecord` values), then `DeferredCompletionQueue::complete` (resolves client futures for committed offsets); (3) The handler collects `IoAction` values into an `IoActionBatch` (e.g., `SendRpc` for the Fetch response, `AppendLog` for newly staged entries); (4) The loop updates the `tokio::sync::watch` channel with the current `ConsensusState` snapshot (§5.11), so concurrent `read()` callers observe the latest state immediately after steps 1–2; (5) The loop calls `self.io_stage.execute(&batch).await` — a direct async method call, not a message to a separate task — which executes storage and network-send operations concurrently via `tokio::join!`; (6) The loop records I/O results (e.g., advancing the durable offset after `AppendLog` completes). Callbacks in step 2 are synchronous, in-process function calls — not external I/O — and always observe the fully updated protocol state before any IoAction is dispatched. Callbacks must be lightweight and non-blocking; applications that need heavy processing should hand off work to their own async tasks. This prevents slow `fsync` calls from delaying Fetch processing and triggering spurious elections. `read()` calls are handled outside this pipeline — they clone the latest value from the `tokio::sync::watch` channel (§5.11) without entering the message queue. |
| **`IoStage`** | Executes `IoAction` batches produced by the `EventLoop` via a direct async method call (`io_stage.execute(&batch).await`). The `IoStage` is **owned** by the event loop task (moved into it at startup); the event loop calls `execute(&self, batch: &IoActionBatch)` inline — no separate task, no message queue. Each action is one of: `PersistQuorumState(QuorumState)`, `AppendLog(Vec<LogEntry>)`, `TruncateSuffix(u64)`, `TruncatePrefix(u64)`, `SendRpc(NodeId, RpcEnvelope)`, `SaveSnapshot(Snapshot)`. The `IoStage` holds owned trait objects (`Box<dyn ...>`) for the injected I/O implementations (`LogStore`, `TransportSender`, `QuorumStateStore`, `SnapshotIO`). No `Arc` wrapping is needed — the `IoStage` is the sole owner, and `execute` takes `&self` so concurrent dispatch within a batch uses shared `&self` borrows (safe because all I/O traits require `Sync`). **Concurrency model:** Within a batch, `execute` partitions actions by trait object and runs them concurrently via `tokio::join!` (e.g., `LogStore::append` runs concurrently with `TransportSender::send` and `QuorumStateStore::save`). Operations on the *same* trait object within one batch are serialised — at most one log-write action (`AppendLog`, `TruncateSuffix`, or `TruncatePrefix`) appears per batch, so no concurrent mutation of a single `LogStore` occurs. All I/O trait methods take `&self` and implementations use interior mutability (e.g., async mutex) for write serialisation. Multiple `SendRpc` actions target different peers and use `TransportSender::send(&self)` concurrently — safe because `TransportSender: Sync`. The event loop awaits the full batch completion before processing the next message; total wait time is `max(storage_latency, network_latency)` because actions run concurrently. **Application callbacks** (`StateMachine::apply`, `Listener::handle_commit`, `Listener::handle_leader_change`) are NOT dispatched by the `IoStage` — they are invoked directly by the `EventLoop` during message processing, immediately after a state change triggers them (e.g., HW advancement during Fetch handling). This ensures callbacks execute synchronously within the event loop's single-threaded context and always see consistent, up-to-date protocol state. The event loop produces the `IoAction` batch *after* callbacks have been invoked, so the Fetch response sent via `IoStage` reflects the same HW that callbacks observed. **Note:** The `IoStage` does NOT call `TransportReceiver` or `Clock` — those are used by the `ReceiverTask` (§4.4) and `EventLoop` respectively. |
| **`BatchAccumulator`** | Stages incoming `propose()` calls into a batch buffer. On each event-loop tick (or when the batch is full), the accumulated entries are drained into a single `AppendLog` I/O action. This amortises `fsync` cost across multiple proposals (group commit). Analogous to KRaft's `BatchAccumulator`. |
| **`DeferredCompletionQueue`** | Parks `tokio::sync::oneshot` senders keyed by log offset. When the high watermark advances, the queue completes all futures whose offset is now **< HW** (strictly less than — see §3.1 canonical HW definition). Analogous to KRaft's `DeferredEventQueue` / purgatory. |
| **`NodeState`** | The full **internal** protocol state (`pub(crate)`), containing: current `term`, `voted_for`, node `role` (Unattached / Follower / Candidate / Leader), the in-memory log index, `high_watermark`, `log_start_offset`, `log_end_offset`, the `voter_set` (as `Vec<VoterInfo>`, consistent with `VotersRecord`), observers, and per-follower replication progress (leader only). The `Unattached` role is the initial state before bootstrap or recovery completes. The **public** type `ConsensusState` returned by `RaftNode::read()` (§5.11) is a **separate, smaller struct** containing only a projected subset: `node_id`, `current_term`, `role`, `leader_id`, `log_end_offset`, `high_watermark`, `voter_set` (committed). The event loop projects `NodeState` → `ConsensusState` after each state mutation and publishes it via a `tokio::sync::watch` channel that `RaftNode::read()` reads from. Internal-only fields are not exposed via `read()`. |
| **`ElectionManager`** | Implements Pre-Vote and Vote protocols. Manages election timeouts (randomised 150–300 ms), vote collection, term advancement, and leader-to-follower step-down on Check Quorum failure. |
| **`ReplicationManager`** | Handles Fetch request/response processing on both leader and follower sides. On the leader: validates fetch offset against the leader-epoch checkpoint, detects log divergence (populates `DivergingEpoch`), tracks follower progress, and advances the high watermark when a majority has replicated. On the follower: sends periodic Fetch RPCs, processes responses, truncates log on divergence, and updates the local high watermark. |
| **`MembershipManager`** | Processes `AddVoter` / `RemoveVoter` / `UpdateVoter` RPCs. Enforces the **single-change invariant**: rejects any membership RPC while an uncommitted `VotersRecord` exists in the log. On `AddVoter`: validates the observer is caught up (`fetch_offset ≥ leader's current HW`), then appends a `VotersRecord` control entry containing the new voter set and stores it as `pending_membership_change` in `NodeState`. On `RemoveVoter`: appends a `VotersRecord` excluding the target node; if the leader is removing itself, it continues serving until the record commits (using the **new** voter set for HW quorum), then steps down to `Unattached`. On `UpdateVoter`: appends a `VotersRecord` with the updated endpoint. The `VotersRecord` travels through the log like any entry — replicated via Fetch, committed when a majority of the **new** voter set has fetched it. **Dual quorum semantics:** Once appended, HW advancement for entries at or after the `VotersRecord`'s offset immediately uses the **new** (pending) voter set (§5.5 quorum transition). Elections, Check Quorum, and the `voter_set` returned by `read()` continue to use the **committed** voter set until the `VotersRecord` is committed. On commit, `voter_set` is atomically replaced by the new set and `pending_membership_change` is cleared. |
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
xraft. They travel through the log like any entry (replicated via Fetch,
committed when a majority has fetched them, included in snapshots) but
are never exposed to the application's `StateMachine::apply`:

- **`LeaderChangeMessage`** — appended by a new leader as the first entry
  of its term (a no-op that establishes commit state for the term). When
  committed, the event loop records the `(term, start_offset)` pair in
  the leader-epoch checkpoint internally. Prior-term entries become
  committable only after this record reaches quorum. Never reaches `apply`.
- **`VotersRecord`** — appended by the leader when processing an
  `AddVoter`, `RemoveVoter`, or `UpdateVoter` RPC. Encodes the complete
  new voter set (not a delta). When committed — using the **new** voter
  set for quorum calculation — the event loop replaces the in-memory
  voter set atomically. The old voter set is discarded. Never reaches
  `apply`. The committed `VotersRecord` is also persisted in snapshot
  metadata so that recovery restores the correct voter set.

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
Included in snapshot metadata for recovery. The `voters` field encodes the
**complete** new voter set (not a delta). **Dual quorum semantics apply:**
Once appended, HW advancement for entries at or after the `VotersRecord`'s
offset uses the new voter set for quorum calculation (see §5.5 Quorum
transition). Elections, Check Quorum, and the `voter_set` returned by
`read()` continue to use the **committed** voter set until the `VotersRecord`
itself is committed. On commit, `NodeState.voter_set` is atomically
replaced by the new set and `pending_membership_change` is cleared; the old
voter set is discarded.

#### `NodeState` (internal consensus state)

> **Naming disambiguation.** The full internal state struct is `NodeState`
> (`pub(crate)` visibility inside `xraft-core`). The **public** type named
> `ConsensusState` returned by `read()` (§5.11) is a **separate, smaller
> struct** containing only a projected subset of these fields. Throughout
> this document, `NodeState` always refers to the internal state; `ConsensusState`
> always refers to the public projected type. The two types are distinct in code.

```
NodeState {
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

    // Voter set (from latest COMMITTED VotersRecord or snapshot)
    voter_set: Vec<VoterInfo>           // the committed voter set — used for
                                        // elections, Check Quorum, and read().
                                        // Replaced atomically when a VotersRecord
                                        // commits. Consistent with snapshot
                                        // metadata voters.
    observers: HashSet<NodeId>

    // Pending membership change (leader-only, at most one)
    pending_membership_change: Option<PendingMembershipChange>
                                        // Set when a VotersRecord is appended but
                                        // not yet committed. Contains the pending
                                        // voter set and the offset of the VotersRecord.
                                        // HW advancement for offsets ≥ this offset
                                        // uses pending_membership_change.voters
                                        // instead of voter_set. Cleared on commit
                                        // (voter_set ← pending voters) or on
                                        // truncation (log divergence discards the
                                        // uncommitted VotersRecord).

    // Leader-only state
    follower_state: HashMap<NodeId, FollowerProgress>

    // Election state
    election_deadline: Instant      // when to start election (follower/candidate)
    votes_received: HashSet<NodeId> // votes collected during election
    pre_votes_received: HashSet<NodeId>
    check_quorum_deadline: Instant  // leader: when to verify quorum
}
```

**Public projection via `read()`.** The `RaftNode::read()` method (§5.11)
clones the latest value from a `tokio::sync::watch` channel that the event
loop updates after each state mutation. The watch value is a **separate
public struct** named `ConsensusState` containing only: `node_id`,
`current_term`, `role`, `leader_id`, `log_end_offset`, `high_watermark`,
and `voter_set` (the **committed** voter set). `ConsensusState` (public)
is a distinct type from `NodeState` (internal). Internal-only fields
(`cluster_id`, `voted_for`, `log_start_offset`, `observers`,
`pending_membership_change`, `follower_state`, election/quorum deadlines,
vote counters) are not exposed through `read()`.

#### `PendingMembershipChange` (leader-only, at most one)

```
PendingMembershipChange {
    offset: u64                     // log offset of the uncommitted VotersRecord
    voters: Vec<VoterInfo>          // the proposed new voter set
}
```

Tracks a `VotersRecord` that has been appended to the log but not yet
committed. The leader uses `voters` for HW advancement (entries at offsets
≥ `offset` require a majority of these voters). Elections, Check Quorum,
and `read().voter_set` continue to use the committed `voter_set`. When the
`VotersRecord` commits, `voter_set ← voters` and this field is cleared.
When log truncation discards the uncommitted `VotersRecord` (divergence
handling), this field is also cleared — the node reverts to the committed
voter set.

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
in a strict sequence: (1) mutate `NodeState`; (2) invoke application
callbacks synchronously if needed (see §4.1 three-phase commit notification);
(3) collect zero or more `IoAction` values into an `IoActionBatch`; (4)
update the `ConsensusState` watch channel (§5.11) so that concurrent
`read()` callers observe the new state immediately after step (1)–(2)
complete. After callbacks, IoAction collection, and watch-channel update
complete, the event loop calls `io_stage.execute(&batch).await` — a
**direct async method call** on the `IoStage`, not a message to a separate
task. The `IoStage` is owned by the event loop task (moved into it at
startup), and `execute` takes `&self` because all I/O trait methods take
`&self` and require `Sync`. The event loop `await`s the full batch
(storage + network sends, running concurrently via `tokio::join!` inside
`IoStage::execute`) before processing the next message. Total wait time
is `max(storage_latency, network_latency)`, not the sum, because all
actions in the batch run concurrently. After `execute` returns, the event
loop records I/O results (e.g., advancing the durable offset). This
staging model keeps the consensus state machine and application callbacks
purely synchronous while allowing external I/O to be parallelised (e.g.,
`fsync` the log and send RPCs concurrently).

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
    ChangeInProgress                // an uncommitted VotersRecord exists in the log
    NodeAlreadyVoter
    NodeNotFound
    NodeNotCaughtUp                 // observer's fetch_offset < leader's current HW
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
| **Storage / Network-Send I/O** (asynchronous) | `LogStore`, `SnapshotIO`, `QuorumStateStore`, `TransportSender` | Dynamic (injected as `Box<dyn ...>` trait objects at construction; moved into the `IoStage`, which is moved into the event loop task) | `Send + Sync + 'static`, `#[async_trait]`, all methods take `&self` | `IoStage` — invoked concurrently across trait objects via `IoStage::execute(&self, batch)` when executing `IoAction` batches (`AppendLog`, `SaveSnapshot`, `PersistQuorumState`, `TruncateSuffix`, `TruncatePrefix`, `SendRpc`). Implementations use interior mutability for write serialisation. `Box<dyn T>` suffices (no `Arc` needed) because the `IoStage` is the sole owner and concurrent access within a batch uses shared `&self` borrows (safe due to `Sync` bound). |
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
   records; control records are filtered. **This is the primary mechanism
   for applications to build their own queryable read-side state** — the
   application processes committed records in the `Listener` callback and
   updates an application-owned data structure (e.g., an `Arc<RwLock<T>>`)
   that can be queried outside of xraft. See §5.11.
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

**Application read-side state model.** xraft does NOT mediate application
state reads. The `StateMachine` is owned by the `EventLoop` and mutated
exclusively by `apply()` during commit processing — applications cannot
safely query it concurrently. Instead, applications build their own
queryable read-side state outside of xraft using one of two patterns:

1. **Listener-driven materialisation (recommended).** The application's
   `Listener::handle_commit` callback receives every committed `AppRecord`
   batch. The application processes these records and updates an
   application-owned data structure (e.g., `Arc<RwLock<HashMap<K, V>>>`)
   that can be queried concurrently from application threads. This is the
   KRaft model — brokers maintain their own metadata cache from committed
   log entries.

2. **Shared state machine wrapper.** The application wraps its state in
   `Arc<RwLock<T>>` and implements `StateMachine` as a thin adapter that
   acquires a write lock in `apply()`. Application threads acquire read
   locks to query state. This requires careful lock ordering to avoid
   blocking the event loop.

Both patterns keep xraft's event loop free of application-specific read
logic and avoid the safety pitfalls of linearizable-read claims (§5.11).

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
    /// Takes `&self` — implementations use interior mutability (e.g.,
    /// `tokio::sync::Mutex<File>`) consistent with the `Send + Sync` bound.
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;

    /// Read entries in [start_offset, end_offset).
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;

    /// Truncate the log suffix starting at the given offset (for divergence).
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;

    /// Truncate the log prefix up to the given offset (after snapshot).
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;

    /// The first offset still in the log.
    fn log_start_offset(&self) -> u64;

    /// The next offset to be written.
    fn log_end_offset(&self) -> u64;

    /// Read the entry at the given offset.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}
```

All `LogStore` mutating methods take `&self` (not `&mut self`), matching
`SnapshotIO::save(&self)` and `QuorumStateStore::save(&self)`. The `IoStage`
holds an owned trait object (`Box<dyn LogStore>`) and invokes it via
`&self` in `IoStage::execute` — concurrently with `TransportSender::send`
calls via `tokio::join!` — safe because `LogStore: Sync`. No `Arc`
wrapping is needed; the `IoStage` is the sole owner (moved into the event
loop task at startup). The `LogStore` implementation serialises its own
write operations internally (e.g., via an async mutex). The `IoStage`
guarantees it will never issue two `LogStore` write operations (`append`,
`truncate_suffix`, `truncate_prefix`) concurrently within the same
`IoActionBatch` — at most one log-write action appears per batch, and
truncation is never combined with append in a single batch.

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
   │ Sender +   │ │ (trait)  │ │  (trait)  │ │  (trait) │ │(trait) │
   │ Receiver   │ │          │ │           │ │          │ │        │
   │ (traits)   │ │          │ │           │ │          │ │        │
   └─────┬──────┘ └────┬─────┘ └─────┬─────┘ └────┬─────┘ └───┬────┘
         │              │             │             │           │
         │              │             │             │           │
         │  ◄── IoStage calls ──►     │             │    EventLoop
         │  (Sender only)            │             │    calls Clock
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

### 4.4 ReceiverTask and Transport Split Design

The transport layer is split into two traits with different ownership
semantics, and the inbound path uses a dedicated `ReceiverTask` that
bridges the network to the `EventLoop`.

#### Split Transport Traits

`TransportSender` and `TransportReceiver` are separate traits (not a
single `Transport`) to resolve ownership conflicts:

- **`TransportSender`** — takes `&self` (shared reference), requires
  `Send + Sync + 'static`. The `IoStage` holds a `Box<dyn TransportSender>`
  and calls `send()` concurrently for multiple peers via `tokio::join!` or
  `FuturesUnordered`. Because `&self` is shared, multiple concurrent sends
  are safe without interior mutability at the trait level.

- **`TransportReceiver`** — takes `&mut self` (exclusive access), requires
  `Send + 'static` (NOT `Sync`). Only the `ReceiverTask` reads from the
  network, so exclusive access is natural and avoids synchronisation overhead.

Both `TcpTransport` (production) and `ChannelTransport` (test) implement
a single struct with a `split()` method:

```rust
fn split(self) -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>)
```

`RaftNode` calls `split()` at construction time and routes each half:
- **Sender** → `IoStage` (for `SendRpc` actions)
- **Receiver** → `ReceiverTask` (for inbound message delivery)

#### ReceiverTask → EventLoop Flow

The `ReceiverTask` is a dedicated async task that bridges network I/O to
the consensus event loop:

```
   Network
     │
     ▼
┌──────────────────┐
│  ReceiverTask    │  async task, owns Box<dyn TransportReceiver>
│  loop {          │
│    msg = recv()  │  calls TransportReceiver::recv(&mut self)
│    tx.send(msg)  │  pushes into tokio::sync::mpsc channel
│  }               │
└────────┬─────────┘
         │ mpsc channel
         ▼
┌──────────────────┐
│   EventLoop      │  drains mpsc, dispatches to handlers
│   (single-       │  mutates NodeState
│    threaded)     │  invokes callbacks
│                  │  emits IoAction batch → IoStage
└──────────────────┘
```

The `ReceiverTask` performs no protocol logic — it only deserialises and
forwards. This keeps all consensus state mutation on the `EventLoop`'s
single thread and avoids shared-state concurrency.

#### What the ReceiverTask Does NOT Do

- It does NOT call `Clock` (timer management is the `EventLoop`'s
  responsibility).
- It does NOT call `LogStore`, `QuorumStateStore`, `SnapshotIO`, or
  `TransportSender` (all external I/O other than receive is dispatched
  via `IoAction` through the `IoStage`).
- It does NOT invoke application callbacks (`StateMachine`, `Listener`).

#### Startup and Shutdown Sequencing

- **Startup:** `RaftNode` completes recovery/bootstrap (§5.9, §5.10) before
  spawning the `ReceiverTask`. This ensures the `EventLoop` has fully
  restored `NodeState` (term, voter set, log bounds) before processing
  any inbound RPCs.
- **Shutdown:** `RaftNode::shutdown()` drops the `ReceiverTask`'s mpsc
  sender (or sends a shutdown signal), causing the task to exit. The
  `EventLoop` drains any remaining messages, invokes
  `Listener::begin_shutdown()`, and then completes pending `IoAction`
  batches before stopping.

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

Adding a new node to the cluster. The node first joins as an **observer**
(non-voting), catches up via Fetch until its `fetch_offset ≥ leader's
current HW`, then is promoted to voter via an `AddVoter` RPC. The RPC
triggers the leader to append a `VotersRecord` control entry containing the
new voter set (old voters + D). The `VotersRecord` is committed using the
**new** voter set for quorum — i.e., a majority of `{A, B, C, D}` must
fetch past the record's offset before HW advances past it.

```
    Admin         Leader           Observer D        Follower B
      │              │                 │                 │
      │              │  ◄── Fetch ─────│ (observer       │
      │              │                 │  replicating)   │
      │              │── FetchResp ───►│                 │
      │              │                 │                 │
      │  (observer D's fetch_offset ≥ leader's HW)      │
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
      │        │    uncommit│          │                 │
      │        │    VotersRe│          │                 │
      │        │    cord?   │          │                 │
      │        │ 3. D.fetch │          │                 │
      │        │    _offset │          │                 │
      │        │    ≥ HW?   │          │                 │
      │        │ 4. Append  │          │                 │
      │        │    Voters- │          │                 │
      │        │    Record  │          │                 │
      │        │  {A,B,C,D} │          │                 │
      │        │    to log  │          │                 │
      │        │ 5. Store   │          │                 │
      │        │    pending │          │                 │
      │        │    membshp │          │                 │
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
      │        │ of NEW set │          │                 │
      │        │ {A,B,C,D}  │          │                 │
      │        │ fetched    │          │                 │
      │        │ VotersRec. │          │                 │
      │        │ HW adv.    │          │                 │
      │        │ Commit:    │          │                 │
      │        │ voter_set  │          │                 │
      │        │ ← {A,B,C,D}│          │                 │
      │        │ Clear      │          │                 │
      │        │ pending.   │          │                 │
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
changes. The check is: scan the log from the last committed offset to LEO;
if any entry has `entry_type = VotersRecord`, reject with `ChangeInProgress`.

**Catch-up threshold:** The leader checks the observer's tracked
`fetch_offset` (from its most recent Fetch request) against the leader's
current `high_watermark`. If `fetch_offset < HW`, the leader rejects the
`AddVoter` with `NodeNotCaughtUp`. This ensures the new voter will not
cause an availability gap by joining the quorum with a stale log.

**Quorum transition (dual semantics).** Once the `VotersRecord` is appended,
the leader stores it as `pending_membership_change` in `NodeState`.
**HW advancement** for entries at or after the `VotersRecord`'s offset
immediately uses the **pending** (new) voter set. This means the new voter
D's `fetch_offset` counts toward commit of the `VotersRecord` itself.
**Elections, Check Quorum, and `read().voter_set`** continue to use the
**committed** `voter_set` until the `VotersRecord` is committed. On commit,
`voter_set` is atomically replaced by the pending voter set, and
`pending_membership_change` is cleared. If log truncation discards the
uncommitted `VotersRecord` (divergence handling after failover),
`pending_membership_change` is cleared and the node reverts to the
committed `voter_set`.

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
`IoStage` executes external I/O (`LogStore::append`, `TransportSender::send`)
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
          │                            │                         │
          │  (Round 1 complete: entries 0,1 delivered.            │
          │   Leader recorded N2.fetch=0, N3.fetch=0 from        │
          │   the Fetch requests. HW calc: [2,0,0], sorted       │
          │   desc → idx 1 = 0. HW stays 0. Two-round           │
          │   visibility applies — §3.1.)                        │
          │                            │                         │
          │  ◄── Fetch(off=2) ────────│  (round 2, N2 has 0-1)  │
          │                            │                         │
    ┌─────┴──────┐                     │                         │
    │ N2.fetch=2 │                     │                         │
    │ HW calc:   │                     │                         │
    │ [A=2,B=2,  │                     │                         │
    │  C=0]      │                     │                         │
    │ sorted →   │                     │                         │
    │ [2,2,0]    │                     │                         │
    │ idx ⌊3/2⌋  │                     │                         │
    │ =1 → 2     │                     │                         │
    │ HW ← 2    │                     │                         │
    │ Majority:  │                     │                         │
    │ leader+N2  │                     │                         │
    │ both ≥ 2.  │                     │                         │
    │ off 0,1    │                     │                         │
    │ committed  │                     │                         │
    │ (0<2,1<2). │                     │                         │
    │ Three-phase│                     │                         │
    │ commit:    │                     │                         │
    │ LCM@0 →    │                     │                         │
    │ internal.  │                     │                         │
    │ VotersRec  │                     │                         │
    │ @1 →       │                     │                         │
    │ internal.  │                     │                         │
    │ No Command │                     │                         │
    │ entries →  │                     │                         │
    │ no SM.apply│                     │                         │
    │ Cluster is │                     │                         │
    │ bootstrapd.│                     │                         │
    └─────┬──────┘                     │                         │
          │                            │                         │
          │── FetchResp(HW=2) ────────►│                         │
          │                            │                         │
          │  ◄── Fetch(off=2) ─────────┼────────────────────────│
          │                            │  (round 2, N3 has 0-1)  │
    ┌─────┴──────┐                     │                         │
    │ N3.fetch=2 │                     │                         │
    │ HW calc:   │                     │                         │
    │ [2,2,2]    │                     │                         │
    │ idx 1 → 2  │                     │                         │
    │ HW stays 2 │                     │                         │
    └─────┬──────┘                     │                         │
          │                            │                         │
          │── FetchResp(HW=2) ─────────┼───────────────────────►│
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
   │  leader_id = N1         │        │
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
   │ d. Scan log from        │        │
   │    log_start_offset     │        │
   │    (here: 81) to        │        │
   │    log_end_offset.      │        │
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
   │    from log_start_off   │        │
   │    to log_end_offset to │        │
   │    build epoch →        │        │
   │    start_offset map)    │        │
   │                         │        │
   │ g. Process control recs │        │
   │    in scanned range for │        │
   │    internal bookkeeping │        │
   │    only:                │        │
   │    - VotersRecord →     │        │
   │      store the LAST one │        │
   │      (highest offset)   │        │
   │      as pending membshp │        │
   │      change. Earlier    │        │
   │      VotersRecords were │        │
   │      committed before   │        │
   │      the next was       │        │
   │      appended (single-  │        │
   │      change invariant). │        │
   │      Do NOT replace     │        │
   │      committed voter_   │        │
   │      set — uncommitted  │        │
   │      VotersRecords are  │        │
   │      not effective for  │        │
   │      elections.          │        │
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

3. **Control records are processed for bookkeeping.** The recovery scan
   covers the range `[log_start_offset, log_end_offset)` — the physical
   extent of the on-disk log. Within this range, `LeaderChangeMessage`
   entries rebuild the leader-epoch checkpoint and `VotersRecord` entries
   are processed for membership bookkeeping. **Scan bounds:** The scan
   starts at `log_start_offset` (the first offset physically present in
   the log after any prior compaction), not at `snapshot.last_included_offset
   + 1`, because `log_start_offset ≥ snapshot.last_included_offset + 1` by
   construction (prefix truncation only removes offsets already covered by
   a snapshot). All entries in this range have **unknown** committed status
   (HW is not persisted). **VotersRecord handling:** If the scan finds one
   or more `VotersRecord` entries, only the **last** one (highest offset)
   is stored as `pending_membership_change`. Earlier `VotersRecord` entries
   in the range were necessarily committed before the next one was appended
   (single-change invariant: at most one uncommitted `VotersRecord` at a
   time), but because HW is unknown during recovery, even these are not
   applied to the committed `voter_set`. When the leader's Fetch response
   later advances HW past earlier `VotersRecord` offsets, the three-phase
   commit notification (§4.1) promotes them sequentially to the committed
   `voter_set`. The last `VotersRecord` remains as `pending_membership_change`
   until HW advances past it (promoting it) or until log truncation discards
   it (clearing it). The committed `voter_set` from the snapshot is never
   overwritten during recovery — only the three-phase commit notification
   modifies it, and only after the leader confirms committed status via HW.

4. **Leader provides the authoritative HW.** After resuming as follower,
   the node sends Fetch requests to the leader. The leader's Fetch
   response includes the current HW. The node advances its local HW to
   `min(leader_HW, local_log_end_offset)` and executes the three-phase
   commit notification (§4.1) for all entries between the old HW and the
   new HW: `StateMachine::apply` for command entries, `Listener::handle_commit`
   for the batch, `DeferredCompletionQueue::complete` (no-op post-crash
   since there are no pending client futures). Control records are always
   filtered from `SM::apply` and handled internally.

5. **Always resume as Follower.** A recovering node never resumes as
   leader regardless of its prior role. It must win a new election.
   All four `QuorumState` fields (`current_term`, `voted_for`,
   `leader_id`, `leader_epoch`) are read from the `quorum-state` file
   before any RPC is processed, preventing double-voting and restoring
   fencing state.

6. **Log integrity via CRC-32C.** Each batch in the log is checksummed.
   The first corrupt or incomplete batch triggers truncation of that batch
   and all subsequent entries. This is safe because entries beyond HW are
   uncommitted by definition (they were not yet replicated to a majority).

7. **Snapshot fallback.** If the recovering node's `fetch_offset` is below
   the leader's `log_start_offset` (the leader has compacted the needed
   entries), the leader responds with a `SnapshotId` and the node falls
   back to snapshot transfer (§5.4).

### 5.11 Client Read (Protocol Metadata)

`RaftNode::read()` returns a projected `ConsensusState` snapshot — the
node's current protocol metadata (term, role, leader ID, high watermark,
log end offset, voter set, node ID). This is a **local, non-linearizable**
read of the node's in-memory state. It does NOT read application state and
does NOT contact other nodes.

> **Cross-document alignment.** All four documents agree: `read() → Result<ConsensusState>`
> is a local, non-linearizable snapshot of protocol metadata; linearisable
> reads are out of scope (tech spec §2.2). Sibling docs list five core fields;
> this architecture adds `log_end_offset` and `node_id` as supplementary fields
> (see §7.3 R4). The impl plan's "routes reads through the log" describes the
> semantic guarantee (returned state reflects HW-committed position); this
> architecture specifies the mechanism (a `tokio::sync::watch` channel updated
> by the event loop after each state mutation). Same observable behaviour —
> see §7.3 R5.

```
    Client            RaftNode         ConsensusState
       │                 │                    │
       │── read() ──────►│                    │
       │                 │── snapshot ───────►│
       │                 │◄── ConsensusState ─│
       │◄── Ok(state) ──│                    │
       │                 │                    │
```

**Read semantics:**

1. **Entry point.** `RaftNode::read()` returns the current `ConsensusState`
   immediately by cloning the latest value from a `tokio::sync::watch`
   channel. It does NOT enter the event loop's message queue, does NOT
   read from the `LogStore`, and does NOT contact other nodes. The
   `RaftNode` struct holds a `watch::Receiver<ConsensusState>`; calling
   `read()` invokes `receiver.borrow().clone()` — a synchronous, lock-free
   operation (watch uses an atomic read-write lock internally).

2. **Watch channel update schedule.** The event loop owns the
   `watch::Sender<ConsensusState>` and calls `sender.send(new_state)` after
   every state mutation (step 4 in the per-message processing order, §2.1).
   This happens **after** `NodeState` is mutated and callbacks are invoked
   (steps 1–2), but **before** the `IoStage` executes external I/O (step 5).
   The watch channel therefore reflects the event loop's latest committed
   protocol position — the same state that callbacks just observed. Because
   the event loop is the sole writer and `watch` provides atomic
   send/receive, there is no data race.

3. **Callable on any node.** Unlike `propose()` (which requires the
   leader), `read()` is callable on any node — leader, follower,
   candidate, or unattached. The returned metadata reflects that node's
   local view, which may be stale relative to the cluster's authoritative
   state.

4. **No linearizability guarantee.** A partitioned node may return an
   outdated `leader_id`, `high_watermark`, or `role`. Callers must treat
   the returned state as a best-effort snapshot. For leader discovery,
   callers should retry on a different node if a `propose()` call returns
   `NotLeader`.

5. **Not an application-state read.** `read()` does NOT query the
   `StateMachine`. Applications build their own queryable read-side state
   from committed records delivered via `Listener::handle_commit` (see
   §4.1 application read-side state model). This separation is deliberate:
   it avoids false linearizability claims and keeps the event loop free of
   application-specific read logic.

**`ConsensusState` fields returned by `read()` (projected subset of `NodeState`, §3.2):**

The following struct is the **public** `ConsensusState` type — a separate,
smaller Rust type from the internal `NodeState` (§3.2). Internal-only fields
(`voted_for`, `cluster_id`, `log_start_offset`, `observers`,
`pending_membership_change`, `follower_state`, election/quorum deadlines,
vote counters) exist only in `NodeState` and are not exposed. The field
names and types below match the corresponding fields in the §3.2 `NodeState`
definition exactly (e.g., `voter_set: Vec<VoterInfo>`, `current_term: Term`).

```rust
pub struct ConsensusState {
    pub current_term: Term,
    pub role: Role,                    // Unattached | Follower | Candidate | Leader
    pub leader_id: Option<NodeId>,
    pub high_watermark: u64,           // exclusive upper bound (§3.1)
    pub log_end_offset: u64,
    pub voter_set: Vec<VoterInfo>,     // committed voter set (§3.2) — does NOT
                                       // include pending membership changes
    pub node_id: NodeId,
}
```

**`RaftNode::read()` proposed signature:**

```rust
impl<S: StateMachine, L: Listener> RaftNode<S, L> {
    /// Read the current protocol state. Returns a local, non-linearizable
    /// snapshot of the node's consensus metadata. Callable on any node.
    ///
    /// Internally clones the latest value from a `tokio::sync::watch` channel
    /// that the event loop updates after each state mutation. Does NOT enter
    /// the event loop message queue, does NOT read from the LogStore, and
    /// does NOT contact other nodes.
    ///
    /// This does NOT read application state — applications maintain their
    /// own read-side state from Listener::handle_commit callbacks (§4.1).
    pub fn read(&self) -> Result<ConsensusState> {
        Ok(self.state_watch_rx.borrow().clone())
    }
}
```

**Relationship to `propose()`.** `propose()` appends a command entry to
the log and waits for HW to advance past it — it is async and
leader-only. `read()` returns local metadata immediately — it is
synchronous and callable on any node. The two are independent.

**Relationship to `metrics()`.** `RaftNode::metrics()` (§6.4) returns
observability counters and latencies (election latency, append rate,
commit latency). `read()` returns protocol state (term, role, HW, voter
set). The two do not overlap: `read()` is for callers making routing or
leadership decisions; `metrics()` is for monitoring dashboards.

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
| **Network failure** | `TransportSender::send()` failures are logged and retried on next tick. The pull-based model is inherently tolerant — a missed Fetch is equivalent to a slow follower. |
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

## 7. Cross-Document Alignment

The four planning documents — this architecture, the tech spec, the
implementation plan, and the e2e scenarios — were authored in **parallel**
by independent agents. They converge on most core design decisions, but
some details still diverge. This section records the shared conventions
and **actively lists every known divergence** so that implementors and
future iterations of any document can reconcile them. Where a divergence
exists, the canonical design is defined by this architecture document.

### 7.1 Shared Conventions (All Four Documents)

| Convention | Detail | Sources |
|------------|--------|---------|
| **Proposed crate layout** | `xraft-core`, `xraft-transport`, `xraft-storage`, `xraft-test`. | tech spec §4.4, impl plan Stage 1.1, e2e preamble |
| **RPC names** | `Vote`, `Fetch`, `FetchSnapshot`, `AddVoter`, `RemoveVoter`, `UpdateVoter`. | tech spec §2.1.4, impl plan Stage 1.3, e2e preamble |
| **Pull-based replication** | Followers `Fetch` from leader; no push-based `AppendEntries`. | tech spec §3, e2e preamble |
| **Serialisation** | `serde` + `bincode`. | tech spec §6 |
| **Control record filtering** | `StateMachine::apply` receives only `AppRecord`; `LeaderChangeMessage` and `VotersRecord` are handled internally. | tech spec §2.1.5, impl plan Stage 1.4, e2e Client Interaction |
| **Snapshot split** | `SnapshotMetadata` (consensus) + `AppSnapshot` (application). | all docs |
| **I/O trait objects** | Storage / Network-Send I/O traits injected as `Box<dyn ...>` — no `Arc`. Trait objects are moved into the `IoStage` at construction; the `IoStage` is moved into the event loop task. `IoStage::execute(&self)` borrows trait objects via `&self` with `Sync` bound for concurrent dispatch. | impl plan Stage 1.7 |
| **Transport split** | Separate `TransportSender` (`&self`, `Sync`) and `TransportReceiver` (`&mut self`, not `Sync`). `split()` on concrete transports. | impl plan Stage 1.4, architecture §4.4 |
| **Clock placement** | `Clock` is a Runtime trait, passed to the `EventLoop` (not `IoStage`), not mediated by `IoAction`. | impl plan Stage 1.4/1.7 |
| **Timing parameters** | 150–300 ms election timeout (randomised), 50 ms fetch interval. | tech spec §4.3 |
| **Quorum math** | Majority = `⌊V/2⌋ + 1`; HW = descending-sorted voter offsets at index `⌊V/2⌋` (0-indexed). Only voters count. During a pending membership change, HW advancement uses the pending (new) voter set for entries at or after the VotersRecord's offset; elections and Check Quorum use the committed voter set. | all docs (HW math); architecture §5.5 and e2e quorum-transition (dual-quorum). Impl plan and tech spec describe the same behaviour without explicitly modelling `pending_membership_change` as a separate field — the e2e scenarios' phrasing "HW uses new voter set on append, elections on commit" confirms semantic agreement (§7.3 R7). |
| **Callback execution model** | Application callbacks (`StateMachine::apply`, `Listener::handle_commit`) are synchronous, in-process calls invoked by the `EventLoop` during message processing, after state mutation but before `IoAction` dispatch. | tech spec §4.4.1, architecture §4.1, impl plan Stages 4.1/5.1, e2e Client Interaction |
| **High watermark (HW) semantics** | Exclusive upper bound: entry at offset O is committed ⟺ `O < HW`. `HW − 1` is the last committed offset. HW is never persisted. | tech spec §8 (glossary), architecture §3.1, impl plan Phase 5, e2e preamble. Tech spec §2.1.1 body uses inclusive phrasing ("entries ≤ N are committed"); this is a notational equivalence: inclusive HW_N maps to exclusive HW = N + 1, producing the same committed set (§7.3 R1). |
| **Commit notification** | Three-phase: (1) `StateMachine::apply`, (2) `Listener::handle_commit`, (3) `DeferredCompletionQueue::complete`. No `DeferredReadQueue`. | architecture §4.1, impl plan Stages 5.1/5.3, e2e Client Interaction |
| **`read()` semantics** | `read() → Result<ConsensusState>` — clones the latest value from a `tokio::sync::watch` channel updated by the event loop after each state mutation. Local, non-linearizable projected subset of protocol metadata (term, role, leader_id, HW, log_end_offset, voter set, node_id). Callable on any node. Does NOT read from `LogStore`, does NOT enter the event loop message queue. Does not read application state. No `StateMachine::query()` method. | tech spec §2.1.5, architecture §5.11, e2e Client Interaction. Sibling docs list 5 core fields (term, role, leader_id, HW, voter set); this architecture adds `log_end_offset` and `node_id` as supplementary fields — abbreviated, not contradictory (§7.3 R4). Impl plan's "routes reads through the log" describes the semantic guarantee (state reflects committed log position); the watch channel is the concrete mechanism (§7.3 R5). |
| **`StateMachine` trait shape** | `apply(offset, &AppRecord)`, `snapshot()`, `restore()` only. No `query()`, no `ReadResult` associated type. | tech spec §2.1.5, architecture §4.1, impl plan Stage 1.4 |
| **`LogStore` method receivers** | All methods take `&self` with interior mutability and `Sync` bound. | architecture §4.1, impl plan Stage 1.4 |
| **Bootstrap & recovery model** | Static voter set → leader commits `VotersRecord`. Recovery: quorum-state (all 4 fields) → snapshot (restores committed `voter_set`) → log scan from `log_start_offset` to `log_end_offset` (metadata only — no SM replay; last `VotersRecord` by offset stored as `pending_membership_change`, not applied to committed `voter_set`; earlier `VotersRecords` are also not applied — promoted sequentially by three-phase commit when HW advances past them) → resume as follower → learn HW from leader → apply entries via three-phase commit notification. | architecture §5.10, impl plan Phase 6, e2e Crash Recovery. Tech spec §2.1.7's "replaying log entries" refers to log restoration/scanning at a higher level of abstraction (§7.3 R2). Impl plan lists `current_term` and `voted_for` as representative fields; `QuorumStateStore::load()` returns the full `QuorumState` struct (§7.3 R3). Impl plan's recovery step "VotersRecord → update voter set" means recovery bookkeeping (storing as `pending_membership_change` for HW purposes), not replacing the committed voter set used for elections (§7.3 R8). Impl plan Stage 6.1 step 4's scan range `log_start_offset` to `log_end_offset` matches this architecture's §5.10 rule 3. |
| **`ClusterId` generation** | Generated once by the operator, passed to `bootstrap()` as a parameter, shared by all nodes. | architecture §5.9, impl plan Stage 6.2. Tech spec §2.1.7 says "generated at bootstrap time" — compatible: the operator generates it at bootstrap time and provides it to `bootstrap()` (§7.3 R6). |
| **Application read-side state** | Applications build their own queryable read-side state from committed records delivered via `Listener::handle_commit`. xraft does not mediate application state reads. | architecture §4.1, e2e Client Interaction |

### 7.2 Verified Resolutions

The following divergences have been confirmed resolved in sibling documents.

| Area | Canonical design (this architecture) | Original divergence | Resolution |
|------|--------------------------------------|---------------------|------------|
| **Callback execution** | Synchronous, in-process calls within the event loop (§4.1). | Tech spec §4.4.1 previously said "asynchronously outside the loop." | Tech spec updated to synchronous model. |
| **`read()` return type name** | `read() → Result<ConsensusState>` (§5.11). | Tech spec §2.1.5 previously said `read() → Result<State>`. | Tech spec updated to `ConsensusState`. |
| **`StateMachine::apply` signature** | `apply(&mut self, offset: u64, record: &AppRecord)` (§4.1). | Tech spec §2.1.5 previously omitted the `offset` parameter. | Tech spec updated to include `offset`. |
| **`LogStore` receivers** | All methods take `&self` with interior mutability (§4.1). | Impl plan Stage 1.4 previously used `&mut self` for write methods. | Impl plan updated to `&self` with `Sync` bound. |
| **Commit phases** | Three-phase: apply → handle_commit → complete (§4.1). | E2e scenarios previously used a four-phase model with `DeferredReadQueue::drain`. | E2e scenarios updated to three-phase model. |
| **`read()` on follower** | Callable on any node, returns local `ConsensusState` (§5.11). | E2e scenarios previously returned `Err(NotLeader)` on followers. | E2e scenarios updated to any-node read. |
| **`StateMachine` trait** | `apply`, `snapshot`, `restore` only — no `query()` (§4.1). | E2e scenarios previously referenced `query()` and `ReadResult`. | E2e scenarios updated. |

### 7.3 Cross-Document Reconciliation Notes

The following items were identified as potential divergences between the four
planning documents. Each has been analysed and reconciled — the documents
are **compatible** once the differing levels of abstraction and notation are
accounted for. No unresolved conflicts remain. Implementors can follow
any of the four documents without encountering contradictory requirements.

| ID | Area | This architecture's detail | Sibling-doc phrasing | Reconciliation |
|----|------|---------------------------|---------------------|----------------|
| **R1** | **HW notation** | Exclusive upper bound: entry committed ⟺ `O < HW` (§3.1). | Tech spec §2.1.1 body says "entries ≤ N are committed" (inclusive notation). Tech spec §8 glossary and impl plan Phase 5 both use exclusive notation. E2e preamble confirms exclusive. | **Notational equivalence.** Inclusive `HW_N` maps to exclusive `HW = N + 1`, producing the same committed set. The impl plan (Phase 5 header) already documents this mapping: "tech-spec HW_inclusive + 1 = architecture HW_exclusive." Implementation uses exclusive throughout. No behavioural difference. |
| **R2** | **Recovery "replay" wording** | No `StateMachine::apply` during recovery. Log entries in the range `[log_start_offset, log_end_offset)` are scanned for control-record metadata only (§5.10 rules 2–4). HW is learned from the leader via Fetch. | Tech spec §2.1.7 step (3) says "replaying log entries after the snapshot offset." | **Precision difference.** The tech spec uses "replaying" at a high level to describe the recovery phase that processes log entries. This architecture specifies the exact semantics: "replay" means scanning from `log_start_offset` to `log_end_offset` for consensus metadata (voter set from `VotersRecord`, epoch checkpoint from `LeaderChangeMessage`) — NOT calling `StateMachine::apply`. The impl plan Stage 6.1 step 4 confirms the same scan range (`log_start_offset` to `log_end_offset`) and "do NOT apply any entries to the `StateMachine`." Both documents describe the same recovery flow; this architecture provides implementation-level precision. |
| **R3** | **Quorum-state field count** | Recovery loads **all 4** `QuorumState` fields: `current_term`, `voted_for`, `leader_id`, `leader_epoch` (§5.10 rule 5). | Impl plan Stage 6.1 step 2 says "load quorum-state file for `current_term` and `voted_for`" — listing 2 of 4 fields. | **Abbreviation.** The impl plan names the two most critical fields for brevity. The `QuorumStateStore::load()` trait (impl plan Stage 1.4) returns `Option<QuorumState>`, and `QuorumState` is defined with all 4 fields (impl plan Stage 1.2). The full struct is loaded — the step description is abbreviated, not incorrect. |
| **R4** | **`read()` field count** | `read()` returns 7 fields: `current_term`, `role`, `leader_id`, `high_watermark`, `log_end_offset`, `voter_set`, `node_id` (§5.11). | Tech spec §2.1.5, e2e scenarios, and impl plan list 5 core fields (term, role, leader ID, HW, voter set). | **Abbreviation.** The 5-field list is the core subset common to all docs. `log_end_offset` and `node_id` are supplementary fields that are trivially available from the same `ConsensusState` struct. The sibling docs abbreviate the field list; they do not assert the struct has *only* 5 fields. No implementation conflict. |
| **R5** | **`read()` implementation path** | `read()` clones the latest `ConsensusState` from a `tokio::sync::watch` channel. The event loop is the sole writer (updates the channel after each state mutation, step 4 in §2.1). `read()` never reads the `LogStore` directly, never enters the event loop's message queue, and never contacts other nodes. The returned state reflects the latest HW-committed protocol position because the event loop updates the watch channel after processing log commits (§5.11). | Impl plan Stages 1.7 and 5.3 say "routes reads through the log for safety, meaning the returned state reflects the latest HW-committed position in the log." | **Semantic guarantee vs. concrete mechanism.** The impl plan describes the *correctness property*: the returned state reflects committed log entries. This architecture specifies the *mechanism* that achieves it: a `watch` channel updated by the event loop (which processes log commits). `read()` does NOT call `LogStore::read()` — "routes through the log" means the event loop *derives* the `ConsensusState` from the log-committed protocol position, then publishes it via the watch channel. The impl plan's phrasing should be read as a semantic guarantee, not a literal implementation directive. Both produce identical observable behaviour. |
| **R6** | **`ClusterId` generation source** | `ClusterId` UUID is generated once by the operator, passed to `bootstrap()` as a parameter (§5.9). | Tech spec §2.1.7 says "generated at bootstrap time." | **Compatible.** "Generated at bootstrap time" accurately describes when the UUID is created — the operator generates it at bootstrap time and passes it to all nodes. The tech spec is ambiguous about *who* generates it; this architecture resolves the ambiguity: the operator generates it (not auto-generated by the code). No conflict. |
| **R7** | **Voter-set dual-quorum semantics** | This architecture distinguishes **committed** `voter_set` (elections, Check Quorum, `read()`) from **pending** `pending_membership_change` (HW advancement only). `NodeState` tracks both fields (§3.2); the public `ConsensusState` exposes only the committed `voter_set`. | E2e scenarios correctly describe the dual semantics: "HW uses new voter set on append, elections on commit." Impl plan and tech spec describe the same behaviour without modelling `pending_membership_change` as a separate field. | **Modelling granularity.** All four documents agree on the *behaviour*: HW advancement switches to the new voter set on append, elections switch on commit. This architecture introduces the `pending_membership_change` field as the implementation-level mechanism. Impl plan and tech spec operate at a higher level where the behaviour is described without naming the internal field. No contradiction — this is implementation detail vs. spec-level description. |
| **R8** | **Recovery VotersRecord handling** | On recovery, uncommitted `VotersRecord` entries are stored as `pending_membership_change` — they do NOT replace the committed `voter_set` from the snapshot (§5.10 rule 3). | Impl plan Stage 6.1 step 5 says "VotersRecord → update voter set." Tech spec §2.1.7 step (3) says "replaying log entries." | **Terminology difference.** The impl plan's "update voter set" refers to recovery-time bookkeeping — storing the `VotersRecord` as `pending_membership_change` so that HW advancement can use it if/when the leader confirms it is committed. It does NOT mean replacing the committed `voter_set` used for elections. The impl plan Stage 8.1 and e2e quorum-transition scenario both confirm that uncommitted `VotersRecord` entries are not effective for elections. This architecture provides the precise semantics: "update" means "store as pending change for HW purposes." |
| **R9** | **Quorum-state schema adaptation** | `QuorumState { current_term, voted_for, leader_id, leader_epoch }` — four fields (§3.2). | Tech spec §2.1.7 lists KRaft's persisted fields: `currentTerm`, `votedFor`, and `votedDirectoryId`. | **KRaft adaptation.** KRaft's `votedDirectoryId` is a Kafka-specific node identity concept. xraft adapts this to two Rust-native fields: `leader_id: Option<NodeId>` (the known leader) and `leader_epoch: Term` (the leader's term, used for fencing in `RpcEnvelope`). This is consistent with the tech spec §2.1.8 KRaft Adaptation Mapping — xraft renames and restructures KRaft mechanisms for its own design. The tech spec's `votedDirectoryId` reference describes the KRaft source material; the architecture's `leader_id` + `leader_epoch` is the adapted xraft design. |
