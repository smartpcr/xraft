# Tech Spec: xraft — Raft Consensus Protocol in Rust

## 1. Problem Statement

Distributed systems require a consensus mechanism to coordinate state across
multiple nodes that may fail independently. The **xraft** project implements
the Raft consensus protocol in Rust, drawing design guidance from Apache
Kafka's KRaft protocol (KIP-500) as described in the Red Hat deep-dive article
and Confluent's KRaft documentation.

The goal is a standalone, library-quality Raft implementation that provides:

- **Leader election** with term-based voting and Pre-Vote protocol.
- **Log replication** with pull-based (fetch) follower synchronisation.
- **Safety guarantees** matching the Raft invariants (leader election safety,
  log matching, state machine safety, leader completeness, append-only leader).
- **Log compaction** via periodic snapshotting.
- **Dynamic quorum changes** (single-node add/remove at a time).

The implementation targets correctness first, then performance. It is not a
Kafka clone — it extracts and adapts the consensus layer described in the
reference material into an independent Rust library and accompanying test
harness.

### Reference Material Assessment

| # | URL | Relevance |
|---|-----|-----------|
| 1 | [Red Hat — Deep dive into Apache Kafka's KRaft protocol](https://developers.redhat.com/articles/2025/09/17/deep-dive-apache-kafkas-kraft-protocol) | **Primary** — provides the authoritative protocol walkthrough covering leader election, log replication, safety rules, Pre-Vote, Check Quorum, snapshotting, dynamic quorum, and implementation architecture. |
| 2 | [Confluent — Learn KRaft](https://developer.confluent.io/learn/kraft/) | **Secondary** — gives architectural context on why KRaft replaced ZooKeeper, the event-sourced storage model, and the pull-based replication design. |
| 3 | [github.com/dragotin/kraft](https://github.com/dragotin/kraft) | **Not relevant** — this repository is a Qt6/KDE invoicing desktop application ("Kraft"), unrelated to the Raft or KRaft consensus protocol. Likely included in the story description by mistake. |

---

## 2. Scope

### 2.1 In Scope

The following capabilities are required for xraft to be considered a complete
implementation of the Raft protocol as described in the reference material.

#### 2.1.1 Core Consensus (Raft)

| Capability | Detail |
|------------|--------|
| **Node roles** | Three states: Follower, Candidate, Leader. Clean state machine transitions driven by timeouts and RPCs. |
| **Leader election** | Term-based election with `RequestVote` (called `Vote` in KRaft). Randomised election timeouts to prevent split votes. |
| **Pre-Vote protocol** | Two-phase election: candidates check viability before incrementing the term, preventing disruptive elections by isolated nodes. |
| **Check Quorum** | Leader periodically verifies it can reach a majority of voters; steps down if quorum is lost. Prevents split-brain during network partitions. |
| **Log replication** | Pull-based model (followers fetch from the leader, as in KRaft), not push-based. Leader tracks follower progress and advances the high watermark when a majority has replicated. |
| **Safety invariants** | Leader election safety, append-only leader, leader completeness, log matching, state machine safety — enforced as described in the Raft paper and the Red Hat article. |
| **Persistence** | `currentTerm`, `votedFor`, and the log are durably persisted to stable storage (file-backed). Voting state stored separately from the log for performance and bootstrapping reasons. |
| **Heartbeats** | Leader sends periodic heartbeats (empty fetch responses or explicit pings) to suppress follower election timeouts. |
| **No-op commit on leader start** | New leader commits a blank entry to establish commit state for the new term before serving reads. |

#### 2.1.2 Log Compaction

| Capability | Detail |
|------------|--------|
| **Snapshotting** | Periodic snapshot of the state machine written to stable storage. Snapshot includes last-applied index, term, and voter set. |
| **Snapshot transfer** | If a leader has discarded log entries a follower needs, it sends the snapshot (chunked transfer). |
| **Log truncation** | After snapshot is taken, log entries before the snapshot index may be discarded. |

#### 2.1.3 Dynamic Quorum (Membership Changes)

| Capability | Detail |
|------------|--------|
| **Single-node changes** | Add or remove one voter at a time to prevent disjoint majorities. |
| **Voter records** | Membership changes committed via a control record in the log (analogous to KRaft's `VotersRecord`). |
| **Non-voting members** | New nodes join as observers (non-voting) until caught up, then promoted to voter. |
| **Leader step-down** | If the leader is removed from the new configuration, it continues to manage until the config change commits, then steps down. |

#### 2.1.4 Transport & RPC

| Capability | Detail |
|------------|--------|
| **RPC framework** | Async message passing between nodes. Four RPC types: `Vote`, `Fetch`, `FetchSnapshot`, and membership-change RPCs. |
| **Identity verification** | Every RPC includes `clusterId` and `currentLeaderEpoch` for fencing stale messages. |
| **Divergence detection** | `Fetch` responses include `DivergingEpoch` when log inconsistency is detected, triggering follower log truncation. |

#### 2.1.5 Observability & Testing

| Capability | Detail |
|------------|--------|
| **Metrics** | Expose key metrics: current leader, current epoch, election latency, append rate, commit latency (mirrors KRaft's raft-metrics). |
| **Deterministic simulation** | Support deterministic testing with injectable clocks, network, and storage to verify correctness under adversarial conditions. |
| **Integration test harness** | Multi-node in-process cluster for scenario-based testing (see `e2e-scenarios.md`). |

### 2.2 Out of Scope

These items are explicitly excluded from the xraft implementation:

| Item | Rationale |
|------|-----------|
| **Kafka-specific metadata** | xraft is a general-purpose Raft library. Topics, partitions, broker registration, and Kafka's `__cluster_metadata` topic are Kafka concerns, not consensus concerns. |
| **Kafka wire protocol** | No compatibility with Kafka's binary protocol. xraft defines its own RPC serialisation. |
| **ZooKeeper migration** | xraft is greenfield; there is no ZooKeeper to migrate from. |
| **Multi-Raft / sharding** | Running multiple independent Raft groups within one process. May be a future extension but is not part of this story. |
| **Client-facing API** | The library exposes a `propose(command)` / `read()` interface. Building a key-value store, database, or any application-layer service on top is out of scope. |
| **Production deployment tooling** | Docker images, Kubernetes operators, Helm charts. |
| **Web UI / dashboard** | Observability is via metrics and structured logs, not a graphical interface. |
| **Linearisable reads** | Read-index or lease-based reads. Initial implementation routes all reads through the log (safe but slower). May be added as a follow-up. |

---

## 3. Non-Goals

These are things the project will intentionally not pursue, even if they could
improve the system:

1. **Kafka compatibility** — xraft is not a Kafka replacement or a KRaft
   reimplementation. It uses KRaft as a design reference, not a compatibility
   target.

2. **Maximum throughput** — correctness and clarity take precedence over raw
   performance. The implementation should be efficient (batching, async I/O),
   but micro-optimisations (lock-free data structures, zero-copy networking)
   are deferred until profiling justifies them.

3. **Pluggable storage engines** — the initial implementation uses a single
   file-backed storage backend. A trait-based storage abstraction exists for
   testability, but supporting multiple production backends (RocksDB, sled) is
   not a goal.

4. **TLS / authentication** — transport security is important for production
   but is not part of the core consensus protocol. The transport layer should
   be designed to allow TLS to be added later without protocol changes.

5. **Language bindings** — no C FFI, Python, or other language wrappers. The
   library is Rust-native.

---

## 4. Hard Constraints

These are non-negotiable requirements that shape every design decision.

### 4.1 Language & Toolchain

| Constraint | Detail |
|------------|--------|
| **Language** | Rust (stable toolchain, latest edition). The story description mandates Rust. |
| **Async runtime** | `tokio` — the de facto standard for async Rust. All I/O (network, disk) is async. |
| **No `unsafe`** | Avoid `unsafe` blocks except where absolutely required for FFI or performance-critical paths, with documented justification and `// SAFETY:` comments. |
| **`#![deny(clippy::all)]`** | All code must pass clippy with no warnings. |
| **`#[must_use]`** | Applied to all Result-returning public functions. |

### 4.2 Correctness

| Constraint | Detail |
|------------|--------|
| **Raft safety invariants** | The five safety properties from the Raft paper (listed in §2.1.1) must hold under all conditions, including crash-recovery and network partition scenarios. |
| **Durable persistence before ack** | Log entries and voting state must be `fsync`-ed to disk before any acknowledgement is sent to peers or clients. |
| **Deterministic testing** | All time-dependent and I/O-dependent behaviour must be injectable for deterministic simulation (see §2.1.5). |
| **Single-threaded event loop** | The core consensus state machine runs on a single-threaded event loop (as in KRaft's `KafkaRaftClient`), avoiding concurrency bugs. External I/O is dispatched to async tasks. |

### 4.3 Project Structure

| Constraint | Detail |
|------------|--------|
| **Workspace layout** | Cargo workspace with separate crates for consensus core, transport, storage, and test harness. See `architecture.md` for crate boundaries. |
| **Repository** | `smartpcr/xraft` — all code lands in this repo. |
| **Branch strategy** | Feature branches off `main`, PR-based review. |

### 4.4 Timing Requirements (from Raft specification)

The system must satisfy the Raft timing invariant:

```
broadcastTime  <<  electionTimeout  <<  avgTimeBetweenFailures
```

| Parameter | Default | Configurable |
|-----------|---------|-------------|
| `broadcastTime` | 0.5–20 ms (measured, not configured) | N/A |
| `electionTimeout` | 150–300 ms (randomised per node) | Yes |
| `heartbeatInterval` | 50 ms | Yes |
| `avgTimeBetweenFailures` | Assumed months+ | N/A |

---

## 5. Identified Risks

### 5.1 Technical Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|-----------|--------|------------|
| R1 | **Subtle consensus bugs** — Raft has many edge cases (e.g., log divergence after leader failure, pre-vote interactions with config changes). Bugs may not manifest until adversarial conditions. | High | Critical | Deterministic simulation testing with fault injection (network partitions, message reordering, crashes at every `fsync` point). Property-based testing for log consistency invariants. |
| R2 | **`fsync` performance on different OSes** — Durable persistence is a hard constraint, but `fsync` latency varies dramatically across platforms and file systems. | Medium | High | Abstract the storage layer behind a trait. Benchmark on Linux ext4/xfs and document minimum requirements. Allow batched `fsync` (group commit) for throughput. |
| R3 | **Pull-based replication complexity** — KRaft's pull-based model (followers fetch from leader) is less common than the push-based model in the original Raft paper. It introduces subtlety around fetch timing, backpressure, and high-watermark advancement. | Medium | High | Implement push-based first as a simpler baseline, then migrate to pull-based. Alternatively, commit to pull-based from the start but invest heavily in integration tests that validate HW advancement under varying fetch rates. |
| R4 | **Snapshot transfer for large state** — Chunked snapshot transfer over the network can be slow and may block normal replication. | Low | Medium | Stream snapshots in chunks with progress tracking. Allow follower to continue fetching log entries that arrive after the snapshot offset while the transfer is in progress. |
| R5 | **Dynamic quorum correctness** — Adding/removing nodes while elections and replication are in flight is the most error-prone part of Raft. | Medium | Critical | Enforce single-node changes only. Extensive scenario testing (see `e2e-scenarios.md`). Consider formal verification of the membership-change state machine (TLA+ spec or Kani model checker for Rust). |
| R6 | **Incomplete reference material** — The third reference URL (`dragotin/kraft`) is an unrelated invoicing application, reducing the available reference implementations to two articles (descriptive, not source code). No Rust reference implementation of KRaft-style Raft exists. | Low | Medium | Supplement with the original Raft paper (Ongaro & Ousterhout, 2014), the `etcd/raft` Go implementation, and the `openraft` Rust crate as cross-references. |

### 5.2 Project Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|-----------|--------|------------|
| P1 | **Scope creep into application layer** — Pressure to build a "useful" system (KV store, message queue) on top of the Raft library before the consensus layer is solid. | Medium | High | Hard scope boundary: this story delivers the consensus library and test harness only. Application layers are separate stories. |
| P2 | **Story points = 0** — Zero story points suggests this may be treated as a spike or exploratory work, but the scope described is a full implementation. | High | Medium | Clarify with the operator whether this is a spike (prototype, time-boxed) or a full delivery. Plan assumes full delivery unless told otherwise. |
| P3 | **Parallel sibling doc drift** — Architecture, implementation plan, and e2e scenarios are written in parallel by sibling architects. Inconsistencies between docs are likely on iteration 1. | High | Low | Cross-reference shared concepts by name. Flag inconsistencies in the iteration summary for alignment in subsequent iterations. |

### 5.3 Risk Heat Map

```
           Low Impact    Medium Impact    High Impact    Critical Impact
          ┌─────────────┬───────────────┬──────────────┬────────────────┐
High      │             │ P2            │ P1           │ R1             │
Likelihood│             │               │              │                │
          ├─────────────┼───────────────┼──────────────┼────────────────┤
Medium    │             │ R4            │ R2, R3       │ R5             │
Likelihood│             │               │              │                │
          ├─────────────┼───────────────┼──────────────┼────────────────┤
Low       │             │ R6            │              │                │
Likelihood│             │               │              │                │
          └─────────────┴───────────────┴──────────────┴────────────────┘
```

---

## 6. Key Design Decisions (Pending)

These decisions affect multiple sibling documents and should be resolved early.
See `architecture.md` for structural decisions and `implementation-plan.md` for
sequencing.

| Decision | Options | Recommendation | Status |
|----------|---------|----------------|--------|
| **Push vs. pull replication** | (A) Push-based as in original Raft, (B) Pull-based as in KRaft | Pull-based (B) — aligns with the KRaft reference material and scales better with observers. | Pending — see `architecture.md` |
| **Async runtime** | (A) tokio, (B) async-std, (C) smol | tokio (A) — ecosystem maturity and library support. | Decided |
| **Serialisation format** | (A) protobuf, (B) flatbuffers, (C) custom binary, (D) serde + bincode | serde + bincode (D) — simplest for Rust-native use; protobuf if cross-language support is needed later. | Pending |
| **State machine interface** | (A) Trait object (`dyn StateMachine`), (B) Generic (`impl StateMachine`) | Generic (B) — zero-cost abstraction, monomorphised at compile time. | Decided |
| **Log storage** | (A) Segment files (Kafka-style), (B) Single append-only file, (C) Embedded DB (sled/rocksdb) | Segment files (A) — natural fit for truncation and compaction. | Pending — see `architecture.md` |

---

## 7. Success Criteria

The implementation is complete when:

1. A 3-node cluster can elect a leader, replicate entries, and survive the
   failure of any single node without data loss.
2. A 5-node cluster can survive the failure of any two nodes.
3. All five Raft safety invariants are verified by deterministic simulation
   tests covering: normal operation, leader failure, network partition,
   log divergence, and snapshot transfer.
4. Dynamic membership changes (add/remove one node) complete without
   violating safety invariants.
5. Pre-Vote and Check Quorum mechanisms prevent disruptive elections by
   isolated nodes.
6. Log compaction via snapshots keeps disk usage bounded under sustained
   write load.
7. Key metrics (leader ID, epoch, election latency, commit latency, append
   rate) are exposed and observable.
8. `cargo test` passes with zero failures. `cargo clippy` reports zero
   warnings. `cargo doc` generates complete API documentation.

---

## 8. Glossary

| Term | Definition |
|------|-----------|
| **Term** | Monotonically increasing integer identifying an election cycle. Acts as a logical clock. |
| **Epoch** | KRaft's name for a term. Used interchangeably in this document. |
| **High watermark (HW)** | The highest log offset replicated by a majority of voters. Entries at or below the HW are considered committed. |
| **Log start offset (LSO)** | The lowest offset still present in the log (entries before this have been compacted/snapshotted). |
| **Voter** | A node that participates in elections and contributes to quorum for commits. |
| **Observer** | A node that replicates the log but does not vote. Analogous to a KRaft broker or a Raft learner. |
| **Pre-Vote** | A two-phase election protocol where a candidate checks viability before incrementing the term. |
| **Check Quorum** | Mechanism where the leader verifies it can reach a majority of voters, stepping down if it cannot. |
| **Diverging epoch** | A fetch response field indicating the follower's log has diverged from the leader's, triggering truncation. |
| **No-op entry** | A blank log entry committed by a new leader to establish its commit state for the new term. |
