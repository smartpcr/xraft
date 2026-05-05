# Tech Spec: xraft — Raft Consensus Protocol in Rust

## 1. Problem Statement

Distributed systems require a consensus mechanism to coordinate state across
multiple nodes that may fail independently. The **xraft** project implements
the Raft consensus protocol in Rust, drawing design guidance from Apache
Kafka's KRaft protocol (KIP-500).

The goal is a standalone, library-quality Raft implementation that provides:

- **Leader election** with term-based voting and Pre-Vote protocol.
- **Log replication** using a pull-based (fetch) model where followers request
  entries from the leader, as described in the KRaft protocol.
- **Safety guarantees** matching the five Raft invariants (leader election
  safety, log matching, state machine safety, leader completeness,
  append-only leader).
- **Log compaction** via periodic snapshotting.
- **Dynamic quorum changes** (single-node add/remove at a time).

The implementation targets correctness first, then performance. It is not a
Kafka clone — it extracts and adapts the consensus layer described in the
reference material into an independent Rust library and accompanying test
harness.

> **Document suite.** This tech spec covers problem, scope, constraints, and
> risks. Structural design lives in [architecture.md](./architecture.md).
> Phased build-out lives in [implementation-plan.md](./implementation-plan.md).
> Behavioural acceptance criteria live in
> [e2e-scenarios.md](./e2e-scenarios.md). Shared concepts use the vocabulary
> defined in §9 (Glossary).
>
> **Terminology convention — term vs. epoch.** The Raft paper uses **term**;
> KRaft uses **epoch** (specifically `currentLeaderEpoch`). This document
> uses **term** as the canonical prose word for the monotonically-increasing
> election counter. The word **epoch** appears only when naming KRaft-derived
> RPC schema fields (`leader_epoch` in `RpcEnvelope`, `DivergingEpoch`,
> `SnapshotId`) or when quoting the reference material. If a sentence says
> "term" and "epoch" side by side, they refer to the same concept. The
> glossary (§9) records both entries and cross-references them.

### 1.1 Reference Material Assessment

Each URL in the story description was reviewed for relevance and content:

| # | URL | Relevance | Key Takeaways |
|---|-----|-----------|---------------|
| 1 | [Red Hat — Deep dive into Apache Kafka's KRaft protocol](https://developers.redhat.com/articles/2025/09/17/deep-dive-apache-kafkas-kraft-protocol) | **Primary** — provides the authoritative protocol walkthrough. | Covers leader election, pull-based log replication via `Fetch` RPCs, safety rules, Pre-Vote, Check Quorum, snapshotting, dynamic quorum (`AddRaftVoter`/`RemoveRaftVoter`/`UpdateRaftVoter`), `DivergingEpoch` for log truncation, high-watermark advancement, single-threaded event loop (`KafkaRaftClient`), `VotersRecord` control records, and the three-layer architecture (`QuorumController` → `KafkaRaftClient` → `MetadataLoader`). |
| 2 | [Confluent — Learn KRaft](https://developer.confluent.io/learn/kraft/) | **Secondary** — gives architectural context. | Explains why KRaft replaced ZooKeeper (operational complexity, scalability, state consistency), the event-sourced storage model, near-instantaneous failover because new leaders already hold committed records in memory, and combined-mode for single-node testing. |
| 3 | [github.com/dragotin/kraft](https://github.com/dragotin/kraft) | **Not relevant** — this repository is a Qt6/KDE invoicing desktop application ("Kraft") for small businesses. It has no connection to the Raft or KRaft consensus protocol. Included in the story description by mistake; ignored. | None. |

> **Note on supplementary references:** Because the story's three URLs yield
> only two relevant sources (both descriptive articles, no reference source
> code), the implementation will also cross-reference:
> - The original Raft paper (Ongaro & Ousterhout, 2014) for formal invariants.
> - The [`etcd/raft`](https://github.com/etcd-io/raft) Go implementation for
>   battle-tested patterns.
> - The [`openraft`](https://github.com/datafuselabs/openraft) Rust crate for
>   idiomatic Rust Raft API design.
> These are used for cross-validation only; xraft is not a port of any of them.

### 1.2 Current Repository State

The `smartpcr/xraft` repository is **greenfield** as of this writing. The
repository contains:

```
README.md          — "# xraft\nimplementation of raft protocol"
docs/              — this document and sibling planning documents (in progress)
```

There is no Rust source code, no `Cargo.toml`, no existing crate structure,
and no prior implementation to extend or refactor. All module names, crate
boundaries, trait definitions, and API signatures described in this document
are **proposed designs** for the initial implementation. They do not reference
existing code.

---

## 2. Scope

### 2.1 In Scope

The following capabilities are required for xraft to be considered a complete
implementation of the Raft protocol as described in the reference material.

#### 2.1.1 Core Consensus (Raft)

| Capability | Detail | Reference |
|------------|--------|-----------|
| **Node roles** | Four roles: **Unattached**, Follower, Candidate, Leader. A node starts as Unattached (no term, no vote, no known leader) and transitions to Follower upon receiving a valid RPC or completing bootstrap/recovery. Clean state machine transitions driven by timeouts and RPCs. The Unattached role is an xraft addition (not in the original Raft paper) that models the pre-bootstrap and post-removal states. | Raft paper §5; Red Hat article "Introducing Raft"; architecture §2.1 |
| **Leader election** | Term-based election with `Vote` RPC (KRaft terminology for `RequestVote`). Randomised election timeouts (150–300 ms) to prevent split votes. A candidate votes for itself, then requests votes from all other nodes. First candidate to receive a majority wins. | Red Hat article "Leader election" |
| **Pre-Vote protocol** | Two-phase election: candidates send a pre-vote request to check viability before incrementing the term. Followers that have recently heard from a leader reject pre-vote requests, preventing disruptive elections by isolated or partitioned nodes. | Red Hat article "Network partition" — Pre-Vote |
| **Check Quorum** | Leader periodically verifies it can communicate with a majority of voters; steps down if quorum is lost. Prevents split-brain during network partitions. | Red Hat article "Network partition" — Check Quorum |
| **Log replication (pull-based)** | Followers and observers periodically send `Fetch` RPCs to the leader to pull new log entries. The leader responds with entries and the current high watermark (HW). This is the KRaft model — not the push-based `AppendEntries` model from the original Raft paper. Pull-based scales better because followers control their own fetch rate and the leader avoids managing outbound connections. Two fetch rounds are needed for a follower to see a commit: one to fetch new records, and a second to receive the updated HW. | Red Hat article "Core RPCs" — Fetch RPC; Confluent article "How it works" |
| **High watermark (HW)** | The leader tracks follower progress (the last offset each follower has fetched). When a majority of voters have replicated up to offset N, the HW advances to N and entries ≤ N are committed. | Red Hat article "Raft replication" step 3 |
| **Safety invariants** | Five invariants enforced at all times: (1) Leader election safety — at most one leader per term. (2) Append-only leader — leader never overwrites/deletes its own entries. (3) Leader completeness — elected leader has all committed entries from prior terms. (4) Log matching — if two logs have an entry with the same index and term, they are identical up to that index. (5) State machine safety — no two nodes apply different entries at the same index. | Red Hat article "Safety rules"; Raft paper §5.4 |
| **Persistence** | `currentTerm`, `votedFor`, and the log are durably persisted to stable storage (`fsync`) before any acknowledgement. Voting state is stored separately from the log for performance and bootstrapping reasons (as in KRaft's `quorum-state` file). | Red Hat article "Safety rules" — persistence requirements |
| **Heartbeats (implicit)** | In the pull-based model, there are no explicit heartbeat messages from leader to follower. Instead, followers send periodic `Fetch` RPCs to the leader; the leader's response — even when there are no new entries — resets the follower's election timeout, serving as an implicit heartbeat. The leader tracks the recency of each follower's `Fetch` requests to detect liveness (this information feeds Check Quorum). If a follower's `Fetch` interval exceeds the election timeout, that follower starts a new election — the leader does not proactively contact it. | Red Hat article "Log replication"; KRaft pull-based model |
| **No-op commit on leader start** | A new leader appends a blank entry (`LeaderChangeMessage` in KRaft) to establish commit state for the new term. Prior-term entries become committed only once this new-term record itself reaches quorum (HW advances past it). This prevents indefinite delays when no client writes occur. | Red Hat article "Log replication"; "Core RPCs" — LeaderChangeMessage |
| **Control records vs application records** | The log contains two classes of entries: (1) **application records** — client-submitted commands forwarded to the `StateMachine`, and (2) **consensus control records** — protocol-internal entries such as `LeaderChangeMessage` and `VotersRecord`. Control records are owned by xraft and are never exposed to the application's `StateMachine::apply`. Snapshots separate consensus metadata (term, vote, voter set, log bounds) from the application state payload. | KRaft design — `VotersRecord`, `LeaderChangeMessage`; Raft paper §7 |

#### 2.1.2 Log Compaction

| Capability | Detail | Reference |
|------------|--------|-----------|
| **Snapshotting** | Periodic snapshot of the state machine written to stable storage. Snapshot includes last-applied index, term, and voter set. Nodes take snapshots independently (logs are consistent, so snapshots are consistent). | Red Hat article "Log compaction" |
| **Snapshot transfer** | If the leader has discarded log entries a follower needs (offset < log start offset), the `Fetch` response includes a `SnapshotId` field. The follower then uses `FetchSnapshot` RPCs to download the snapshot in chunks. | Red Hat article "Core RPCs" — SnapshotId; FetchSnapshot |
| **Log truncation** | After a snapshot is taken, log entries before the snapshot index may be discarded. The log start offset (LSO) advances accordingly. | Red Hat article "Log compaction" |

#### 2.1.3 Dynamic Quorum (Membership Changes)

| Capability | Detail | Reference |
|------------|--------|-----------|
| **Single-node changes** | Add or remove one voter at a time to prevent disjoint majorities. Enforced by the leader. xraft's RPCs: `AddVoter`, `RemoveVoter`, `UpdateVoter` (analogous to KRaft's `AddRaftVoter` / `RemoveRaftVoter` / `UpdateRaftVoter`). | Red Hat article "Dynamic quorum" |
| **Voter records** | Membership changes committed via a control record in the log (analogous to KRaft's `VotersRecord`). The voter set is part of the snapshot for recovery. | Red Hat article "Dynamic quorum" — VotersRecord |
| **Non-voting members (observers)** | New nodes join as observers (non-voting) until caught up with the leader, then promoted to voter. Observers replicate the log via `Fetch` but do not contribute to quorum. This avoids availability gaps when a new node has an empty log. | Red Hat article "Cluster scaling" |
| **Leader step-down** | If the leader is removed from the new configuration, it continues managing the cluster until the voters-record commits or the epoch advances, then steps down. | Red Hat article "Dynamic quorum" |

#### 2.1.4 Transport & RPC

| Capability | Detail | Reference |
|------------|--------|-----------|
| **RPC framework** | Async message passing between nodes. Six RPC types defined by xraft: `Vote` (election), `Fetch` (log replication), `FetchSnapshot` (snapshot transfer), `AddVoter`, `RemoveVoter`, `UpdateVoter` (membership changes). These are xraft's names; the KRaft equivalents are `Vote`, `Fetch`, `FetchSnapshot`, `AddRaftVoter`, `RemoveRaftVoter`, and `UpdateRaftVoter` respectively. | Red Hat article "Core RPCs" and "Dynamic quorum" |
| **Identity & fencing** | Every RPC includes `clusterId` and `currentLeaderEpoch` for identity verification and fencing of stale messages. | Red Hat article "Core RPCs" — "All KRaft RPC schemas include `clusterId` and `currentLeaderEpoch`" |
| **Divergence detection** | `Fetch` responses include a `DivergingEpoch` tagged field when log inconsistency is detected. The follower truncates its log back to the diverging point. Multiple fetch rounds may be required in worst-case scenarios. | Red Hat article "Core RPCs" — DivergingEpoch |
| **Leader-epoch checkpoint** | The leader validates fetch requests against its log using a leader-epoch checkpoint (cached in memory for efficiency). This enables fast divergence detection. | Red Hat article "Core RPCs" — `leader-epoch-checkpoint` |

> **RPC direction summary (pull-based model):** In xraft, the leader never
> initiates outbound RPCs to followers for log replication or heartbeats.
> All data flow for replication is follower-initiated via `Fetch` RPCs.
> The only leader-to-follower data is carried in `Fetch` *responses*.
> Candidates initiate `Vote` RPCs during elections (this is push, but election
> is a distinct phase from steady-state replication). `FetchSnapshot` is also
> follower-initiated. Membership-change RPCs (`AddVoter` / `RemoveVoter` /
> `UpdateVoter`) are client-to-leader requests, not leader-to-follower pushes.

#### 2.1.5 Library API (Proposed Design)

xraft will expose a **Rust library API** for embedding into applications. The
signatures below are the proposed initial design; they will be refined during
implementation. This is a compile-time linked library, not a network service.

| Capability | Proposed Signature |
|------------|--------------------|
| **`propose(command) → Future<Result>`** | Submit a command to the replicated log. Returns a future that resolves when the entry is committed (HW has advanced past it). The API is **at-least-once**: after leader failover or client timeout, a retry may append the same logical command twice. Applications must ensure commands are idempotent (e.g., via application-level request IDs / dedup). xraft does not perform built-in deduplication. |
| **`read() → Result<ConsensusState>`** | Read the node's current protocol metadata (term, role, leader ID, high watermark, voter set). Returns a local, non-linearizable snapshot of the node's consensus state. Callable on any node — leader, follower, candidate, or unattached. Does not read application state; applications build their own read-side state from committed records delivered via `Listener::handle_commit` (see architecture §5.11). |
| **Listener trait** | Applications implement callbacks: `handle_commit(batch)`, `handle_load_snapshot(reader)`, `handle_leader_change(leader_id, term)`, `begin_shutdown()`. Modelled on KRaft's `RaftClient.Listener` interface. |
| **State machine trait** | `trait StateMachine { fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>; fn snapshot(&self) -> Result<AppSnapshot>; fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>; }` — applications provide their own state machine. The `apply` method includes the committed entry's log offset for idempotency and checkpointing. The trait receives only **application records** (`AppRecord`); consensus control records are handled internally by xraft and never reach `apply`. Snapshots are split: xraft owns consensus metadata (term, voter set, log bounds); the application owns its payload via `AppSnapshot`. Generic (monomorphised at compile time) for zero-cost abstraction. |

> **Scope boundary:** xraft provides the consensus library and test harness.
> Building a key-value store, message queue, or any application-layer service
> on top of the library is out of scope for this work.

#### 2.1.6 Observability & Testing

| Capability | Detail | Reference |
|------------|--------|-----------|
| **Metrics** | Expose key metrics mirroring KRaft's `raft-metrics` group: `current-leader`, `current-epoch`, `election-latency-avg`, `append-records-rate`, `commit-latency-avg`. Exposed as Rust structs; integration with Prometheus/OpenTelemetry is an extension concern. | Red Hat article "KRaft protocol" — metrics list |
| **Deterministic simulation** | Support deterministic testing with injectable clocks, network, and storage to verify correctness under adversarial conditions. | — |
| **Integration test harness** | Multi-node in-process cluster for scenario-based testing. Scenarios cover leader failure, network partition, log divergence, snapshot transfer, and membership changes. | — |

#### 2.1.7 Bootstrap & Recovery

| Capability | Detail | Reference |
|------------|--------|-----------|
| **Cluster bootstrap** | Initial cluster formation from a static voter set. The first leader commits a `VotersRecord` control record during bootstrap. Nodes identify the cluster via a `clusterId` UUID generated at bootstrap time. | KRaft bootstrap — `VotersRecord`; Red Hat article "Metadata management" |
| **Persistent quorum state** | Each node persists `currentTerm`, `votedFor`, and `votedDirectoryId` in a `quorum-state` file separate from the log, read at startup before any RPCs are processed. | Red Hat article "Metadata management" — `quorum-state` file |
| **Crash recovery** | On restart, a node recovers by: (1) reading `quorum-state` for term/vote, (2) loading the most recent snapshot (if any) for state machine and voter set, (3) replaying log entries after the snapshot offset, (4) resuming as a follower. | Raft paper §5.2; KRaft recovery model |
| **Empty/uninitialized node** | A new node with no log and no snapshot joins as an observer. If it is behind the leader's LSO, it receives a snapshot via `FetchSnapshot` before normal log replication can proceed. | Red Hat article "Core RPCs" — SnapshotId |

#### 2.1.8 KRaft Adaptation Mapping

The following table clarifies which KRaft mechanisms xraft adopts, adapts, or
intentionally omits. This prevents ambiguity about the design's relationship
to the reference material.

| KRaft Mechanism | xraft Status | Notes |
|-----------------|-------------|-------|
| Pull-based `Fetch` replication | **Adopted** | Core replication model; see §2.1.1 |
| `Vote` RPC (two-phase with Pre-Vote) | **Adopted** | Includes Pre-Vote and Check Quorum |
| `FetchSnapshot` chunked transfer | **Adopted** | Follower-initiated; see §2.1.2 |
| `DivergingEpoch` log truncation | **Adopted** | Fetch response field for divergence detection |
| `LeaderChangeMessage` no-op | **Adopted** | Control record on leader start; prior-term entries commit once this record reaches quorum |
| `VotersRecord` for membership | **Adopted** | Control record in log + snapshot |
| `AddRaftVoter` / `RemoveRaftVoter` / `UpdateRaftVoter` | **Adopted** | Renamed `AddVoter` / `RemoveVoter` / `UpdateVoter` |
| `leader-epoch-checkpoint` | **Adopted** | In-memory cache for fast divergence detection |
| High watermark (HW) commit tracking | **Adopted** | Leader tracks follower fetch offsets |
| `BatchAccumulator` staged append | **Adapted** | xraft uses a similar batching buffer; exact API differs |
| `DeferredEventQueue` (purgatory) | **Adapted** | Client futures parked until HW advances past their offset |
| Single-threaded `KafkaEventQueue` | **Adapted** | xraft consensus core uses single-threaded async event loop (§4.4) |
| Three-layer architecture | **Adapted** | xraft maps to: Application API → Consensus Core (`xraft-core`) → State Machine apply pipeline |
| `__cluster_metadata` topic | **Not adopted** | Kafka-specific; xraft uses a generic replicated log |
| `metadata.version` / feature gates | **Not adopted** | Kafka versioning scheme; not needed for single-purpose library |
| `BrokerRegistration` / `BrokerHeartbeat` | **Not adopted** | Kafka broker lifecycle; observers replicate via `Fetch` only |
| Kafka wire protocol (binary format) | **Not adopted** | xraft uses `serde` + `bincode` (§6) |
| `NO_OP_RECORD` (KIP-835) | **Not adopted** | Kafka internal; xraft uses `LeaderChangeMessage` equivalent only |

### 2.2 Out of Scope

These items are explicitly excluded from the xraft implementation:

| Item | Rationale |
|------|-----------|
| **Kafka-specific metadata** | xraft is a general-purpose Raft library. Topics, partitions, broker registration, `__cluster_metadata` topic, and Kafka metadata versioning (`metadata.version`) are Kafka concerns. |
| **Kafka wire protocol** | No compatibility with Kafka's binary protocol. xraft defines its own RPC serialisation. |
| **ZooKeeper migration** | xraft is greenfield; there is no ZooKeeper to migrate from. |
| **Multi-Raft / sharding** | Running multiple independent Raft groups within one process. May be a future extension but is not part of this story. |
| **Network service endpoint** | xraft does not expose an HTTP, gRPC, or other network-accessible service API. It is an embedded library. Applications that need a network-facing consensus service build that layer on top of the library API (§2.1.5). |
| **Production deployment tooling** | Docker images, Kubernetes operators, Helm charts. |
| **Web UI / dashboard** | Observability is via metrics structs and structured logs, not a graphical interface. |
| **Linearisable reads** | Read-index or lease-based reads. Initial implementation routes all reads through the log. May be added as a follow-up. |
| **TLS / authentication** | Transport security is not part of the consensus protocol. The transport layer is designed for TLS to be added later without protocol changes. |
| **Language bindings** | No C FFI, Python, or other language wrappers. The library is Rust-native. |

---

## 3. Non-Goals

These are things the project will intentionally not pursue, even if they could
improve the system:

1. **Kafka compatibility** — xraft is not a Kafka replacement or a KRaft
   reimplementation. It uses KRaft as a design reference for pull-based
   replication and dynamic quorum, not a compatibility target. It does not
   implement Kafka's metadata topic, broker registration, or metadata versioning.

2. **Push-based replication** — The original Raft paper uses push-based
   `AppendEntries` RPCs. xraft commits exclusively to the pull-based (fetch)
   model from KRaft. This is a deliberate design choice, not a deferral.
   The pull-based model is more complex to implement but scales better with
   observers and simplifies leader connection management (the leader does not
   need outbound connections to every follower). Hybrid push-pull or fallback
   to push-based is not planned.

3. **Maximum throughput** — correctness and clarity take precedence over raw
   performance. The implementation should be efficient (batching, async I/O),
   but micro-optimisations (lock-free data structures, zero-copy networking)
   are deferred until profiling justifies them.

4. **Pluggable storage engines** — the initial implementation uses a single
   segment-file-based storage backend (see §6, Log storage decision in Key
   Design Decisions). A trait-based storage abstraction exists for testability,
   but supporting multiple production backends (RocksDB, sled) is not a goal.

5. **Language bindings** — no C FFI, Python, or other language wrappers. The
   library is Rust-native.

---

## 4. Hard Constraints

These are non-negotiable requirements imposed by the story description, the
Raft protocol specification, or fundamental correctness requirements. They
cannot be relaxed without changing the project's mandate.

### 4.1 Language

| Constraint | Detail | Source |
|------------|--------|--------|
| **Rust** | The implementation language is Rust. Minimum Supported Rust Version (MSRV): **1.75** (the first stable release with `async fn` in traits via `impl Trait`). Rust edition: **2021** (consistent with the `Cargo.toml` workspace settings in the implementation plan). Stable toolchain only — no nightly features. | Story description: "implement raft using rust". |
| **Repository** | All code lands in `smartpcr/xraft`. | Story description and repository context. |

### 4.2 Protocol Correctness

| Constraint | Detail | Source |
|------------|--------|--------|
| **Raft safety invariants** | The five safety properties from the Raft paper (listed in §2.1.1) must hold under all conditions, including crash-recovery and network partition scenarios. | Raft paper §5.2–5.4; Red Hat article "Safety rules". |
| **Failure model: crash-recovery + partitions** | xraft assumes the **non-Byzantine, crash-recovery** failure model: nodes may crash and restart (losing only volatile state), messages may be delayed, reordered, duplicated, or lost, and network partitions may occur. Byzantine failures (malicious or arbitrary behaviour) are explicitly out of scope. This is the standard Raft assumption. | Raft paper §2; distributed systems convention. |
| **Node identity: no NodeId reuse** | Each `NodeId` is unique for the lifetime of a cluster. A node removed via `RemoveVoter` cannot rejoin the same cluster with the same `NodeId`. Replacement nodes must use a new `NodeId`. This eliminates the need for a `DirectoryId` / incarnation counter and simplifies vote-fencing logic. KRaft's `votedDirectoryId` mechanism is intentionally not adopted. | Design decision — simplifies §2.1.3 and recovery. |
| **Durable persistence before ack** | Log entries and voting state must be `fsync`-ed to disk before any acknowledgement is sent to peers or clients. | Raft paper §5; Red Hat article "Safety rules" — persistence requirements. |
| **Pull-based replication** | Log replication uses the pull-based (fetch) model where followers request entries from the leader. Push-based `AppendEntries` is not implemented. | Story description reference to KRaft; §3 Non-Goals item 2. |
| **Timing invariant** | `broadcastTime << electionTimeout << avgTimeBetweenFailures`. | Raft paper §5.6. |
| **Full production quality** | This is a complete, production-quality implementation — not a time-boxed spike or prototype. All features (election, replication, compaction, dynamic quorum) must be correct and tested. | Project requirement. |

### 4.3 Timing Parameters

The system must satisfy the Raft timing invariant:

```
broadcastTime  <<  electionTimeout  <<  avgTimeBetweenFailures
```

| Parameter | Default | Configurable |
|-----------|---------|-------------|
| `broadcastTime` | 0.5–20 ms (measured, not configured) | N/A |
| `electionTimeout` | 150–300 ms (randomised per node) | Yes |
| `fetchInterval` | 50 ms (follower's periodic Fetch RPC interval) | Yes |
| `avgTimeBetweenFailures` | Assumed months+ | N/A |

> **Note on heartbeats:** In the pull-based model there are no explicit
> heartbeat messages from leader to follower (see §2.1.1). The `fetchInterval`
> parameter controls how often followers send `Fetch` RPCs to the leader. The
> leader's response — even when empty — resets the follower's election timer,
> serving as an implicit heartbeat. The Raft timing invariant still applies:
> `fetchInterval << electionTimeout << avgTimeBetweenFailures`.

### 4.4 Implementation Commitments

The following are engineering commitments adopted based on judgment and
alignment with the reference material. They shape the implementation and may
be revisited if evidence warrants, but changing them requires explicit
justification.

#### 4.4.1 Protocol & Correctness

| Commitment | Detail | Rationale |
|------------|--------|-----------|
| **Async runtime: `tokio`** | All I/O (network, disk) is async, using the `tokio` runtime. | De facto standard for async Rust; broadest ecosystem support. |
| **Single-threaded event loop (non-blocking)** | The core consensus state machine runs on a single-threaded event loop (as in KRaft's `KafkaRaftClient`). The consensus loop mutates protocol state, then invokes application callbacks (`StateMachine::apply`, `Listener::handle_commit`) synchronously within the loop (in-process calls, not external I/O), then produces `IoAction` values describing external I/O. An `IoStage` executes those actions concurrently via injected trait objects (disk `fsync`, network sends). This staging prevents slow I/O from delaying `Fetch` handling and triggering spurious elections. Follows the KRaft pattern of `BatchAccumulator` (stages records before draining) and `DeferredEventQueue` (parks client futures until commit). See architecture §4.1 for the three-phase commit notification model. | Eliminates concurrency bugs in the consensus core; matches KRaft's architecture. Prevents election-timeout-triggered instability under I/O load. |
| **Deterministic testing** | All time-dependent and I/O-dependent behaviour must be injectable for deterministic simulation (see §2.1.6). | Enables reproducible testing of edge cases that are impossible to trigger reliably with wall-clock time and real I/O. |
| **Workspace layout** | Proposed Cargo workspace with separate crates: `xraft-core` (consensus state machine), `xraft-transport` (async RPC), `xraft-storage` (durable log and snapshots), and `xraft-test` (deterministic simulation harness). These crates do not exist yet — the repository is greenfield (see §1.2). | Separation of concerns; enables independent testing and versioning of each layer. |

#### 4.4.2 Code Quality

| Commitment | Detail | Rationale |
|------------|--------|-----------|
| **No `unsafe`** | Avoid `unsafe` blocks except where absolutely required for FFI or performance-critical paths, with documented justification and `// SAFETY:` comments. | Reduces the surface area for memory-safety bugs in a correctness-critical system. |
| **`#![deny(clippy::all)]`** | All code must pass clippy with no warnings. | Catches common Rust pitfalls early. |
| **`#[must_use]`** | Applied to all Result-returning public functions. | Prevents silent error swallowing. |

#### 4.4.3 Process

| Commitment | Detail | Rationale |
|------------|--------|-----------|
| **Branch strategy** | Feature branches off `main`, PR-based review. | Standard collaborative development workflow. |

### 4.5 Assumptions

The following assumptions are implicit prerequisites for correctness and
liveness. If any assumption is violated, the safety or liveness guarantees of
the protocol may not hold.

| Assumption | Detail | Consequence if violated |
|------------|--------|------------------------|
| **Local disk honours `fsync` semantics** | After a successful `fsync()` call, the data is durable on the storage medium. The file system supports atomic rename (write-to-temp, `fsync`, rename) for snapshot commits. | Committed entries or voting state may be silently lost on crash, violating Raft safety. |
| **Monotonic clocks with bounded drift** | Each node has access to a monotonic clock source (`tokio::time::Instant`) with drift bounded well below `electionTimeout`. Wall-clock synchronisation (NTP) is not required — Raft uses logical clocks (terms). | Unbounded clock regression could cause premature election timeouts or Check Quorum failures, reducing availability (not safety). |
| **Application callbacks are non-blocking** | `StateMachine::apply`, `Listener::handle_commit`, and other application callbacks return quickly (< 1 ms). Applications that need heavy processing hand off work to their own async tasks. | Blocking callbacks stall the event loop, delaying Fetch responses and risking election-timeout expirations across the cluster. |
| **Unique `NodeId` provisioning** | `NodeId` values are provisioned externally and are unique within a cluster for its entire lifetime. A removed node never rejoins with the same `NodeId`. | Duplicate `NodeId` would corrupt vote-fencing and follower-progress tracking, violating leader election safety. |
| **Non-Byzantine failure model** | Nodes may crash and restart (losing only volatile state). Messages may be delayed, reordered, duplicated, or lost. Network partitions may occur. Nodes do not exhibit Byzantine (malicious or arbitrary) behaviour. | Byzantine behaviour (e.g., sending fabricated vote responses) can violate all safety invariants. |
| **Reliable local storage (no silent corruption)** | Disk sectors do not silently corrupt data without detectable errors. CRC-32C checksums (§6) detect corruption; the assumption is that the underlying hardware reports I/O errors faithfully. | Silent bit-rot undetected by CRC could apply corrupt entries to the state machine. |

### 4.6 Compatibility and Stability Policy

xraft is a new, greenfield implementation. The following stability commitments
apply during initial development (pre-1.0):

| Surface | Stability | Notes |
|---------|-----------|-------|
| **Public Rust API** (`RaftNode`, `StateMachine`, `Listener`, `propose`, `read`) | **Unstable** — may change between minor versions. | Semantic versioning with `0.x.y` during initial development. Breaking changes documented in changelogs. |
| **On-disk log format** (segment files, CRC layout) | **Unstable** — no backward-compatible migration guaranteed pre-1.0. | Snapshot + log format may evolve. Upgrade path: re-snapshot from running cluster before upgrading. |
| **On-disk quorum-state format** | **Unstable** — same policy as log format. | Small file; easy to re-derive from cluster state after upgrade. |
| **Wire format** (RPC serialisation via `serde` + `bincode`) | **Unstable** — `bincode` is not self-describing and not backward-compatible across schema changes. | All nodes in a cluster must run the same xraft version. Rolling upgrades are not supported pre-1.0. |
| **Metrics struct** | **Unstable** — field names and types may change. | Consumers should not rely on stable field names until 1.0. |

> **Post-1.0 goal:** Establish backward-compatible on-disk and wire formats
> (potentially switching to a self-describing serialisation format like
> `postcard` or `protobuf`) to enable rolling upgrades. This is out of scope
> for the initial implementation.

---

## 5. Identified Risks

### 5.1 Technical Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|-----------|--------|------------|
| R1 | **Subtle consensus bugs** — Raft has many edge cases (e.g., log divergence after leader failure, pre-vote interactions with config changes). Bugs may not manifest until adversarial conditions. | High | Critical | Deterministic simulation testing with fault injection (network partitions, message reordering, crashes at every `fsync` point). Property-based testing for log consistency invariants. |
| R2 | **`fsync` performance on different OSes** — Durable persistence is a hard constraint, but `fsync` latency varies dramatically across platforms and file systems. | Medium | High | Abstract the storage layer behind a trait. Benchmark on Linux ext4/xfs and document minimum requirements. Allow batched `fsync` (group commit) for throughput. |
| R3 | **Pull-based replication complexity** — KRaft's pull-based model (followers fetch from leader) is less common than the push-based model in the original Raft paper. It introduces subtlety around fetch timing, backpressure, and high-watermark advancement. | Medium | High | Commit to pull-based from the start (see §3 Non-Goals — push-based is explicitly excluded). Invest heavily in integration tests that validate HW advancement under varying fetch rates. Build a deterministic simulation harness early so fetch-timing edge cases are reproducible. Cross-reference `etcd/raft` and `openraft` for replication state-machine patterns, adapted to pull semantics. |
| R4 | **Snapshot transfer for large state** — Chunked snapshot transfer over the network can be slow and may block normal replication. | Low | Medium | Stream snapshots in chunks with progress tracking. Allow follower to continue fetching log entries that arrive after the snapshot offset while the transfer is in progress. |
| R5 | **Dynamic quorum correctness** — Adding/removing nodes while elections and replication are in flight is the most error-prone part of Raft. Concurrent leader election and membership change can create disjoint-majority hazards if the single-change invariant is violated (the Red Hat article explicitly warns: "asynchronous metadata log replication with multiple in-flight quorum changes could create disjoint majorities"). | Medium | Critical | Enforce single-node changes only. Reject any membership RPC while an uncommitted `VotersRecord` exists in the log. Extensive scenario testing covering add-during-election, remove-leader, add-while-partitioned, and simultaneous-add-and-remove-attempt cases. Formal verification: use TLA+ for quorum-transition and state-machine modelling (protocol-level reasoning); use Kani model checker or property-based tests (proptest) for bounded Rust implementation checks. |
| R6 | **Incomplete reference material** — The third reference URL (`dragotin/kraft`) is an unrelated invoicing application, reducing the available reference implementations to two articles (descriptive, not source code). No Rust reference implementation of KRaft-style Raft exists. | Low | Medium | Supplement with the original Raft paper (Ongaro & Ousterhout, 2014), the `etcd/raft` Go implementation, and the `openraft` Rust crate as cross-references. |
| R7 | **Log/snapshot corruption and torn writes** — Crash during `fsync` can leave partial writes in the log or snapshot files. Recovery must handle truncated segments, partial records, and corrupt CRC checksums without data loss beyond the uncommitted tail. | Medium | Critical | Every log segment and snapshot chunk includes CRC-32C checksums. On recovery, scan forward and truncate at first corrupt/incomplete record. Use `O_DSYNC` or explicit `fsync` before advancing any durable offset. Write snapshot atomically via rename (write to temp file, `fsync`, rename). |
| R8 | **Memory/backpressure under load** — Unbounded pending proposals, snapshot transfers in flight, or large fetch-lag deltas can exhaust memory. The pull-based model amplifies this: if followers are slow, the leader accumulates entries with no push-side backpressure. | Medium | High | Bound the proposal queue (reject with `Busy` if full). Cap in-flight snapshot transfers per node. Limit the batch size in `Fetch` responses. Monitor log-end-offset minus HW as a lag metric. |
| R9 | **Two-round commit visibility latency** — In the pull-based model, a follower needs two `Fetch` rounds to see a newly committed entry: one to replicate it, and a second to receive the updated HW. Under low fetch rates this can add perceptible latency. | Low | Medium | Document the two-round property. Tune `fetchInterval` to be much smaller than `electionTimeout`. Consider adaptive fetch frequency (faster when writes are in flight). |

### 5.2 Project Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|-----------|--------|------------|
| P1 | **Scope creep into application layer** — Pressure to build a "useful" system (KV store, message queue) on top of the Raft library before the consensus layer is solid. | Medium | High | Hard scope boundary: this work delivers the consensus library and test harness only (§2.2). Application layers are out of scope. |
| P2 | **Underestimated effort** — A full production-quality Raft implementation (leader election, pull-based replication, snapshotting, dynamic quorum, deterministic simulation) is substantial engineering work. Underestimation could lead to quality shortcuts. | Medium | High | Prioritise correctness over velocity. Ship the consensus core first (election + replication + persistence), then layer on compaction, dynamic quorum, and simulation testing incrementally. |
| P3 | **Cross-document alignment** — Multiple planning documents (tech spec, architecture, implementation plan, e2e scenarios) may be authored in parallel for this story. If they exist, inconsistencies between them may emerge across iterations. | Medium | Low | Each document should stand alone. Shared concepts use consistent naming (e.g., RPC names from §2.1.4, crate names from §4.4). Flag inconsistencies in iteration summaries so subsequent iterations can reconcile. |

### 5.3 Risk Heat Map

```
           Low Impact    Medium Impact    High Impact    Critical Impact
          ┌─────────────┬───────────────┬──────────────┬────────────────┐
High      │             │               │              │ R1             │
Likelihood│             │               │              │                │
          ├─────────────┼───────────────┼──────────────┼────────────────┤
Medium    │ P3          │ R4            │ R2, R3, R8,  │ R5, R7         │
Likelihood│             │               │ P1, P2       │                │
          ├─────────────┼───────────────┼──────────────┼────────────────┤
Low       │             │ R6, R9        │              │                │
Likelihood│             │               │              │                │
          └─────────────┴───────────────┴──────────────┴────────────────┘
```

---

## 6. Key Design Decisions

These decisions affect the overall design and are recorded here as the
authoritative source. Structural details (proposed crate boundaries, module
layouts) and sequencing details (implementation phases, milestones) belong in
the architecture and implementation-plan documents if they are produced.

| Decision | Options | Recommendation | Status |
|----------|---------|----------------|--------|
| **Push vs. pull replication** | (A) Push-based as in original Raft, (B) Pull-based as in KRaft | Pull-based (B) — aligns with the KRaft reference material and scales better with observers. | **Decided** — see §3 Non-Goals item 2. |
| **Async runtime** | (A) tokio, (B) async-std, (C) smol | tokio (A) — ecosystem maturity and library support. | **Decided** |
| **Serialisation format** | (A) protobuf, (B) flatbuffers, (C) custom binary, (D) serde + bincode | serde + bincode (D) — simplest for Rust-native use; protobuf if cross-language support is needed later. | **Decided** — Rust-native scope (§2.2) makes bincode the natural choice. |
| **State machine interface** | (A) Trait object (`dyn StateMachine`), (B) Generic (`impl StateMachine`) | Generic (B) — zero-cost abstraction, monomorphised at compile time. | **Decided** |
| **Log storage** | (A) Segment files (Kafka-style), (B) Single append-only file, (C) Embedded DB (sled/rocksdb) | Segment files (A) — natural fit for truncation and compaction. Each segment covers a range of offsets. | **Decided** — aligns with KRaft's segment-based log and §3 Non-Goals item 4 (single backend). |
| **Event-loop pattern** | (A) Blocking single thread, (B) Non-blocking single-threaded async loop with staged I/O | Non-blocking async loop (B) — consensus state mutations are synchronous within the loop; application callbacks (`StateMachine::apply`, `Listener::handle_commit`) are invoked synchronously within the loop as in-process calls, not offloaded to async tasks; disk I/O, snapshot streaming, and network sends are offloaded to the `IoStage` which executes them concurrently. Follows KRaft's `KafkaEventQueue` / `BatchAccumulator` / `DeferredEventQueue` pattern. | **Decided** — see §4.4. |
| **Proposal batching** | (A) One-at-a-time append, (B) Batched accumulator with drain | Batched accumulator (B) — proposals are staged in a `BatchAccumulator` and drained to the log on a configurable interval or when the batch is full. Improves throughput by amortising `fsync` cost (group commit). | **Decided** — adapted from KRaft's `BatchAccumulator`. |
| **Client commit notification** | (A) Polling, (B) Future/channel per proposal, (C) Callback | Future per proposal (B) — each `propose()` call returns a `Future` that resolves when HW advances past the entry's offset. Internally implemented via a deferred-completion queue (adapted from KRaft's `DeferredEventQueue`). | **Decided** |
| **Log integrity** | (A) No checksums, (B) CRC per record, (C) CRC per batch | CRC-32C per batch (C) — each batch written to a log segment includes a CRC-32C checksum. On recovery, the log is scanned forward; the first batch with a bad CRC triggers truncation of that batch and everything after it. | **Decided** — see risk R7 mitigation. |

---

## 7. Success Criteria

The implementation is complete when:

1. A 3-node cluster can elect a leader, replicate entries, and survive the
   failure of any single node without data loss. Leader election completes
   within 2× `electionTimeout` after a leader crash.
2. A 5-node cluster can survive the failure of any two nodes while continuing
   to commit entries.
3. All five Raft safety invariants are verified by deterministic simulation
   tests covering: normal operation, leader failure, network partition,
   log divergence, snapshot transfer, and crash-recovery. Zero committed
   entries are lost across any crash/restart matrix.
4. Dynamic membership changes (add/remove one node) complete without
   violating safety invariants. Membership change propagation: the leader
   applies the new voter set immediately on append; a follower that already
   holds the `VotersRecord` applies it on the next successful `Fetch` that
   advances HW past its offset; a follower that has not yet replicated the
   record requires up to two successful `Fetch` rounds in steady state
   (one to replicate the record, one to receive the updated HW).
5. Pre-Vote and Check Quorum mechanisms prevent disruptive elections by
   isolated nodes. Verified by partition simulation tests.
6. Log compaction via snapshots keeps disk usage bounded: at most one segment
   beyond the latest snapshot offset is retained under sustained write load.
7. Key metrics (leader ID, epoch, election latency, commit latency, append
   rate) are exposed and validated by integration tests that assert on
   metric values (not just existence).
8. `cargo test` passes with zero failures. `cargo clippy` reports zero
   warnings. `cargo doc` generates complete API documentation.
9. Crash recovery from log + snapshot + `quorum-state` file restores a node
   to a consistent state and it rejoins the cluster as a follower.

---

## 8. Cross-Document Consistency

This section tracks alignment with sibling planning documents. Shared
terminology (RPC names, crate names, role names, offset conventions) is
defined in this tech spec and consumed by the siblings.

| Topic | This document | Sibling | Status |
|-------|---------------|---------|--------|
| Crate names | `xraft-core`, `xraft-storage`, `xraft-transport`, `xraft-test` (§4.4) | architecture.md §2, implementation-plan.md Phase 1 | **Aligned** |
| RPC names | `Vote`, `Fetch`, `FetchSnapshot`, `AddVoter`, `RemoveVoter`, `UpdateVoter` (§2.1.4) | architecture.md §3, e2e-scenarios.md header | **Aligned** |
| Roles | Unattached, Follower, Candidate, Leader (§2.1.1) | architecture.md §3.1, e2e-scenarios.md header | **Aligned** |
| Offset semantics | HW is exclusive upper bound; `fetch_offset` is exclusive (§8 Glossary) | architecture.md §3.1, e2e-scenarios.md offset conventions | **Aligned** |
| Rust edition | **2021** (§4.1) | implementation-plan.md Stage 1.1: `edition = "2021"` | **Aligned** (fixed in this iteration — was "latest edition") |
| Term vs. epoch | **term** canonical; **epoch** for KRaft field names only (preamble §1) | architecture.md uses both | **Needs review** — architecture.md should adopt the same convention |

> **Policy:** If a future iteration of any sibling document diverges from the
> shared vocabulary above, the inconsistency should be flagged in that
> document's Iteration Summary so the next iteration of either document can
> reconcile.

---

## 9. Glossary

| Term | Definition |
|------|-----------|
| **Term** | Monotonically increasing integer identifying an election cycle. Acts as a logical clock. This is the canonical word used in xraft prose. See also: *Epoch*. |
| **Epoch** | KRaft's name for a term. In xraft, **epoch** appears only in KRaft-derived RPC field names (`leader_epoch`, `DivergingEpoch`, `SnapshotId.epoch`) and when quoting reference material. In all other contexts, use **term**. See preamble §1. |
| **High watermark (HW)** | An exclusive upper bound on committed offsets. Entry at offset O is committed ⟺ `O < HW`. Equivalently, `HW − 1` is the last committed offset. HW = 0 means no entries are committed. See architecture §3.1 for canonical definition. |
| **Log start offset (LSO)** | The lowest offset still present in the log (entries before this have been compacted/snapshotted). |
| **Log end offset (LEO)** | The offset of the next entry to be appended. `LEO - 1` is the last entry in the log. |
| **Voter** | A node that participates in elections and contributes to quorum for commits. |
| **Observer** | A node that replicates the log but does not vote. Analogous to a KRaft broker or a Raft learner. |
| **Quorum** | A majority of voters (⌊N/2⌋ + 1 out of N voters). Required for election and commit. |
| **Pre-Vote** | A two-phase election protocol where a candidate checks viability before incrementing the term. |
| **Check Quorum** | Mechanism where the leader verifies it can reach a majority of voters, stepping down if it cannot. |
| **Diverging epoch** | A fetch response field indicating the follower's log has diverged from the leader's, triggering truncation. |
| **Leader-epoch checkpoint** | An in-memory index mapping each epoch to its start offset, used for fast divergence detection during `Fetch` validation. |
| **No-op entry** | A blank log entry appended by a new leader to establish its commit state for the new term. |
| **Control record** | A log entry owned by the consensus layer (e.g., `LeaderChangeMessage`, `VotersRecord`), not exposed to the application state machine. |
| **Application record** | A log entry containing a client-submitted command, forwarded to the application `StateMachine::apply`. |
| **Cluster ID** | A UUID assigned at cluster bootstrap time, included in every RPC for identity verification. |
| **Snapshot** | A point-in-time capture of the state machine plus consensus metadata (term, voter set, last-applied offset), used for log compaction and node catch-up. |
| **BatchAccumulator** | A staging buffer that collects proposed entries before draining them to the log in a single batch (group commit). |
| **DeferredEventQueue** | A completion queue that parks client futures until the HW advances past their entry's offset (adapted from KRaft's purgatory pattern). |
