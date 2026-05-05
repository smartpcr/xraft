# E2E Scenarios: xraft — Raft Consensus Protocol in Rust

> **Story:** `raft:RAFT-PROTOCOL`
>
> **Sibling documents:** [tech-spec.md](./tech-spec.md) ·
> [architecture.md](./architecture.md) ·
> [implementation-plan.md](./implementation-plan.md)
>
> This document defines Gherkin-style end-to-end scenarios for the xraft
> library. Every scenario is executable against the **deterministic simulation
> harness** described in the tech spec (§2.1.6). Scenarios use the crate and
> RPC names established in the tech spec (§2.1.4) and architecture doc (§3):
>
> - Crates: `xraft-core`, `xraft-transport`, `xraft-storage`, `xraft-test`
> - RPCs: `Vote` (with `is_pre_vote` flag for Pre-Vote phase), `Fetch`,
>   `FetchSnapshot`, `AddVoter`, `RemoveVoter`, `UpdateVoter`
> - Roles: Unattached, Follower, Candidate, Leader
> - Membership: Voter or Observer (non-voting). Observer is a membership
>   classification — not a role in the state machine. An observer node
>   runs in Follower role but is in the observers set, not the voter set.
> - Pull-based replication model (followers `Fetch` from leader; no push-based
>   `AppendEntries`)
> - At-least-once propose semantics; deduplication is the application's
>   responsibility (tech spec §2.1.5)
>
> **Offset conventions** (per architecture doc §3.2 and §5.2):
> - Log offsets are **0-based** (first entry at offset 0).
> - `fetch_offset` is **exclusive**: a follower with `fetch_offset = N`
>   has replicated entries `[0, N)` and wants entries starting at N.
> - High watermark (HW) is an **exclusive upper bound**: entries with
>   `offset < HW` are committed. `HW − 1` is the last committed offset.
>   HW = 0 means no entries are committed.
> - Where the tech spec describes "entries ≤ N committed", that
>   corresponds to `HW = N + 1` in exclusive notation.

---

## Table of Contents

1. [Feature: Leader Election](#feature-leader-election)
2. [Feature: Pre-Vote Protocol](#feature-pre-vote-protocol)
3. [Feature: Pull-Based Log Replication](#feature-pull-based-log-replication)
4. [Feature: High Watermark Advancement](#feature-high-watermark-advancement)
5. [Feature: Log Divergence and Truncation](#feature-log-divergence-and-truncation)
6. [Feature: Check Quorum](#feature-check-quorum)
7. [Feature: Persistence and Crash Recovery](#feature-persistence-and-crash-recovery)
8. [Feature: Log Compaction and Snapshots](#feature-log-compaction-and-snapshots)
9. [Feature: Snapshot Transfer](#feature-snapshot-transfer)
10. [Feature: Cluster Bootstrap](#feature-cluster-bootstrap)
11. [Feature: Dynamic Quorum — Add Voter](#feature-dynamic-quorum--add-voter)
12. [Feature: Dynamic Quorum — Remove Voter](#feature-dynamic-quorum--remove-voter)
13. [Feature: Dynamic Quorum — Observer Promotion](#feature-dynamic-quorum--observer-promotion)
14. [Feature: Client Interaction](#feature-client-interaction)
15. [Feature: Safety Invariants](#feature-safety-invariants)
16. [Feature: Observability and Metrics](#feature-observability-and-metrics)

---

## Feature: Leader Election

```gherkin
Feature: Leader Election
  As a Raft cluster
  I need exactly one leader per term
  So that all state changes are coordinated through a single authority

  Background:
    Given a 3-node cluster [N1, N2, N3] with all nodes in Follower state
    And election timeouts are randomised between 150ms and 300ms
    And no leader exists

  Scenario: Initial leader election in a fresh cluster
    Given all nodes start simultaneously
    When node N1's election timeout expires first
    Then N1 transitions to Candidate state
    And N1 increments its term to 1
    And N1 votes for itself
    And N1 sends Vote RPCs to N2 and N3
    When N2 and N3 grant their votes to N1
    Then N1 transitions to Leader state for term 1
    And N1 appends a no-op LeaderChangeMessage entry to its log
    And N2 and N3 remain in Follower state

  Scenario: Leader election with competing candidates (split vote)
    Given N1 and N2 both have their election timeouts expire simultaneously
    When N1 sends Vote RPCs with term 1
    And N2 sends Vote RPCs with term 1
    And N1 votes for itself and receives N3's vote (2 of 3 — majority)
    And N2 votes for itself but receives no additional votes (1 of 3)
    Then N1 becomes Leader for term 1
    And N2 transitions back to Follower state
    And N2 recognises N1 as the leader

  Scenario: Election timeout triggers new election after split vote with no majority
    Given N1 and N2 both become candidates for term 1
    And neither N1 nor N2 receives a majority of votes
    When both candidates' election timeouts expire again
    Then a new election begins with term 2
    And randomised timeouts make it unlikely both expire simultaneously again
    And eventually one candidate wins a majority and becomes Leader

  Scenario: Follower rejects vote for stale term
    Given N1 is Leader for term 3
    When N2 receives a Vote RPC from N3 with term 2
    Then N2 rejects the vote because term 2 < current term 3
    And N2's state is unchanged

  Scenario: Follower has already voted in current term
    Given N2 has already voted for N1 in term 4
    When N2 receives a Vote RPC from N3 for term 4
    Then N2 rejects the vote because it has already voted for N1 this term
    And N2's votedFor remains N1

  Scenario: Candidate with incomplete log is rejected
    Given N1's log contains entries up to offset 10, term 3
    And N2's log contains entries up to offset 8, term 3
    When N2 becomes a candidate and sends Vote RPCs for term 4
    Then N1 rejects the vote because N2's log is less up-to-date
    And leader completeness invariant is preserved

  Scenario: Candidate steps down on receiving higher term
    Given N1 is a Candidate for term 5
    When N1 receives a Vote RPC response from N2 with term 6
    Then N1 transitions to Follower state
    And N1 updates its current term to 6
    And N1 clears its votedFor

  Scenario: Leader election in a 5-node cluster
    Given a 5-node cluster [N1, N2, N3, N4, N5]
    When N1 becomes a Candidate for term 1
    And N1 receives votes from N2 and N3 (3 of 5 — majority)
    Then N1 becomes Leader for term 1
    And N1 does not need votes from N4 or N5

  Scenario: Follower receiving Vote RPC with higher term updates its term
    Given N2 is a Follower with currentTerm 3
    When N2 receives a Vote RPC from N1 with term 7
    And N1's log is at least as up-to-date as N2's
    Then N2 updates its currentTerm to 7
    And N2 grants its vote to N1
    And N2 resets its election timeout
```

---

## Feature: Pre-Vote Protocol

```gherkin
Feature: Pre-Vote Protocol
  As a Raft cluster with the Pre-Vote optimisation
  I need candidates to check viability before incrementing the term
  So that isolated or partitioned nodes cannot disrupt the cluster
  with unnecessary elections

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And Pre-Vote is enabled (default configuration)

  Scenario: Successful pre-vote followed by full election
    Given N1 has not heard from a leader within the election timeout
    When N1 initiates a pre-vote phase
    Then N1 sends Vote RPCs with is_pre_vote=true to N2 and N3 without incrementing its term
    When N2 and N3 respond positively (N1's log is up-to-date)
    Then N1 proceeds to the full election
    And N1 increments its term
    And N1 sends Vote RPCs with is_pre_vote=false to N2 and N3

  Scenario: Isolated node cannot disrupt cluster via pre-vote rejection
    Given N1 is Leader for term 5
    And N3 is partitioned from N1 but can reach N2
    And N2 has recently received a Fetch response from N1
    When N3's election timeout expires
    And N3 sends Vote RPCs with is_pre_vote=true to N1 and N2
    Then N2 rejects the Vote(is_pre_vote=true) because it has recently heard from the leader
    And N1 is unreachable from N3
    And N3 cannot gather a pre-vote majority
    And N3 does NOT increment its term
    And the cluster continues operating normally with N1 as leader

  Scenario: Pre-vote prevents term inflation by isolated node
    Given N3 is completely isolated from N1 and N2
    When N3's election timeout expires repeatedly
    Then N3 sends Vote RPCs with is_pre_vote=true each time
    And all Vote(is_pre_vote=true) RPCs time out (no reachable nodes)
    And N3's term remains unchanged
    And when the partition heals, N3 can rejoin without forcing a new election

  Scenario: Pre-vote succeeds when leader has truly failed
    Given N1 was Leader for term 3 but has crashed
    And N2 and N3 have not received a Fetch response within the election timeout
    When N2 initiates a pre-vote
    And N2 sends Vote RPCs with is_pre_vote=true to N1 and N3
    And N3 has not heard from the leader recently
    Then N3 grants the pre-vote to N2
    And N2 proceeds to a full election with term 4
    And N2 wins and becomes the new Leader

  Scenario: Pre-vote responder grants vote only if candidate log is up-to-date
    Given N1 is partitioned from the cluster (but not crashed)
    And N2's log contains entries up to offset 5, term 3
    And N3's log contains entries up to offset 8, term 3
    When N2 sends Vote RPCs with is_pre_vote=true for term 3
    Then N3 rejects the pre-vote because N2's log (offset 5) is less up-to-date than N3's (offset 8)
    And N2 cannot proceed to a full election
```

---

## Feature: Pull-Based Log Replication

```gherkin
Feature: Pull-Based Log Replication
  As a Raft cluster using the KRaft pull-based replication model
  I need followers to periodically Fetch entries from the leader
  So that all nodes maintain consistent replicated logs without
  requiring the leader to manage outbound connections

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is the Leader for term 1
    And the follower fetch interval is configured to 50ms

  Scenario: Single entry replication via Fetch
    Given no application entries have been proposed (the log contains only the LeaderChangeMessage at offset 0)
    When a client proposes command "set x=1" to N1
    Then N1 appends "set x=1" at offset 1, term 1 to its log
    When N2 sends a Fetch RPC to N1 with fetch_offset 1
    Then N1 responds with entry [offset 1, term 1, "set x=1"]
    And N2 appends the entry to its local log
    When N3 sends a Fetch RPC to N1 with fetch_offset 1
    Then N1 responds with entry [offset 1, term 1, "set x=1"]
    And N3 appends the entry to its local log

  Scenario: Batch replication — multiple entries in one Fetch
    Given the leader N1 has application entries at offsets 1–5 (offset 0 is LeaderChangeMessage)
    And N2 has only replicated up to offset 0
    When N2 sends a Fetch RPC to N1 with fetch_offset 1
    Then N1 responds with entries [1, 2, 3, 4, 5]
    And N2 appends all 5 entries to its local log in order

  Scenario: Follower Fetch acts as implicit heartbeat
    Given N1 is Leader and no new commands are proposed
    When N2 sends a Fetch RPC to N1 with fetch_offset equal to its log end offset
    Then N1 responds with an empty entry set and the current high watermark
    And N2's election timeout is reset
    And N2 remains in Follower state

  Scenario: Follower starts election after missed Fetches
    Given N1 is Leader
    And N2 cannot reach N1 (network fault)
    When N2 has not received a Fetch response for longer than the election timeout
    Then N2 transitions to Candidate state
    And N2 initiates a new election

  Scenario: Leader detects stale epoch via leader-epoch checkpoint
    Given N1 is Leader for term 3
    And N1's leader-epoch checkpoint maps term 1 → start offset 0, term 3 → start offset 15
    When N2 sends a Fetch RPC with last_fetched_epoch = 1 and fetch_offset = 20
    Then N1 consults its leader-epoch checkpoint and determines epoch 1 ended at offset 15
    And N1 responds with DivergingEpoch { epoch: 1, end_offset: 15 }
    And N2 truncates its log from offset 15 onward
    And N2 resumes Fetching from offset 15 with updated last_fetched_epoch

  Scenario: Fetch includes cluster_id for identity verification
    Given the cluster has cluster_id "cluster-abc-123"
    When N2 sends a Fetch RPC with cluster_id "cluster-xyz-999"
    Then N1 rejects the Fetch due to cluster_id mismatch
    And the rejection prevents cross-cluster contamination

  Scenario: Leader responds with leader identity in Fetch response
    Given N1 is Leader for term 3
    When N2 sends a Fetch RPC to N1
    Then the Fetch response includes leader_id=N1 and leader_epoch=3
    And N2 can confirm it is Fetching from the correct leader
```

---

## Feature: High Watermark Advancement

```gherkin
Feature: High Watermark Advancement
  As a Raft leader
  I need to advance the high watermark when a majority has replicated an entry
  So that entries become committed and can be applied to state machines

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is the Leader for term 1
    And the quorum size is 2 (majority of 3)

  # HW is exclusive (per architecture doc §5.2): entries with offset < HW
  # are committed. fetch_offset is also exclusive: a follower with
  # fetch_offset = N has entries [0, N). The leader records fetch_offset on
  # each incoming Fetch request and recalculates HW by sorting all voters'
  # fetch_offsets descending, taking the value at index ⌊V/2⌋.

  Scenario: High watermark advances after majority Fetch (3-node)
    Given N1 has entries at offsets 0 (LeaderChangeMessage) and 1 (command), term 1
    And N1's log end offset is 2
    And the high watermark (HW) is 1 (offset 0 committed; HW is exclusive)
    When N2 sends a Fetch RPC with fetch_offset 2 (N2 has entries [0, 2))
    Then N1 records N2's fetch_offset as 2
    And N1 recalculates HW: sorted desc [N1=2, N2=2, N3=0] → index 1 → 2
    And N1 advances the HW from 1 to 2 (entries with offset < 2 now committed)
    And entry at offset 1 is now committed (1 < HW=2)
    When N3 sends a Fetch RPC with fetch_offset 0
    Then N3 receives entries at offsets 0–1 and HW = 2
    And N3 applies the command entry at offset 1 to its state machine

  Scenario: Two Fetch rounds required for follower to see commit
    Given N1 has entries at offsets 0 (LeaderChangeMessage) and 1 (command), term 1
    And N1's log end offset is 2
    And HW is 1 (only offset 0 committed)
    # Round 1: N2 fetches the data but HW does not advance
    When N2 sends a Fetch RPC with fetch_offset 1 (N2 has entries [0, 1))
    Then N1 records N2's fetch_offset as 1
    And N1 recalculates HW: sorted desc [N1=2, N2=1, N3=0] → index 1 → 1
    And HW remains 1 (entry at offset 1 is NOT committed: 1 ≮ 1)
    And N1 responds with entry at offset 1 and HW = 1
    And N2 appends entry 1 but does NOT apply it (offset 1 is not < HW)
    # Round 2: N2 confirms replication, HW advances
    When N2 sends another Fetch RPC with fetch_offset 2 (N2 now has entries [0, 2))
    Then N1 records N2's fetch_offset as 2
    And N1 recalculates HW: sorted desc [N1=2, N2=2, N3=0] → index 1 → 2
    And N1 advances HW to 2
    And N1 responds with HW = 2
    And N2 applies entry at offset 1 to its state machine (1 < HW=2)

  Scenario: HW does not advance without majority
    Given a 5-node cluster [N1, N2, N3, N4, N5]
    And N1 is Leader with entries at offsets 0–1 (log end offset 2)
    And HW is 1 (offset 0 committed)
    When only N2 has confirmed replication with fetch_offset 2
    And N3, N4, N5 still have fetch_offset 0
    Then N1 recalculates HW: sorted desc [2, 2, 0, 0, 0] → index 2 → 0
    And HW remains at 1 (HW never decreases)
    And entry at offset 1 is NOT committed (1 is not < HW=1)

  Scenario: HW advances in a 5-node cluster with 3 replicas
    Given a 5-node cluster [N1, N2, N3, N4, N5]
    And N1 is Leader with entries at offsets 0–1 (log end offset 2)
    And HW is 1 (offset 0 committed)
    When N2 and N3 each send Fetch RPCs with fetch_offset 2 (both have entries [0, 2))
    Then N1 recalculates HW: sorted desc [2, 2, 2, 0, 0] → index 2 → 2
    And N1 advances HW to 2 (entries with offset < 2 are committed)
    And entry at offset 1 is committed
    And N4 and N5 do not need to Fetch for the entry to be committed

  Scenario: No-op entry committed on leader start advances HW
    Given N1 has just won election for term 2
    And N1 appends a no-op LeaderChangeMessage at offset 5, term 2
    And N1's log end offset is 6
    When N2 sends Fetch with fetch_offset 6 (N2 has entries [0, 6))
    Then N1 recalculates HW: majority of voters have fetch_offset ≥ 6
    And N1 advances HW to 6 (entries with offset < 6 are committed)
    And all entries through offset 5 (including prior-term entries) are now committed
```

---

## Feature: Log Divergence and Truncation

```gherkin
Feature: Log Divergence and Truncation
  As a Raft cluster
  I need to detect and resolve log divergence between leader and follower
  So that the log matching invariant is maintained

  Background:
    Given a 3-node cluster [N1, N2, N3]

  Scenario: Follower truncates divergent entries on DivergingEpoch
    Given N1 was Leader for term 1 and appended entries at offsets 1–5
    And N2 replicated entries 1–3 but N1 crashed before entries 4–5 were committed
    And N3 becomes Leader for term 2 and appends new entries at offsets 4–6
    When N2 sends a Fetch RPC to N3 (new leader)
    And N3 detects that N2's entry at offset 4 has term 1 instead of term 2
    Then N3 responds with DivergingEpoch indicating the divergence point
    And N2 truncates its log from offset 4 onward
    And N2 Fetches the correct entries at offsets 4–6 from N3

  Scenario: Multiple Fetch rounds to resolve deep divergence
    Given N2 has entries at offsets 1–10 from terms [1,1,1,2,2,2,3,3,3,3]
    And the new leader N3 has entries at offsets 1–8 from terms [1,1,1,2,2,2,4,4]
    When N2 sends a Fetch RPC to N3
    Then N3 responds with DivergingEpoch for term 3 (N2's entries at offsets 7–10)
    And N2 truncates offsets 7–10
    When N2 sends another Fetch RPC
    Then N3 responds with entries at offsets 7–8 from term 4
    And N2's log now matches the leader's

  Scenario: Leader validates Fetch against leader-epoch-checkpoint
    Given N1 is Leader for term 5
    And N1 maintains a leader-epoch-checkpoint mapping epochs to start offsets
    When N2 sends a Fetch RPC with last_fetched_epoch = 3 and fetch_offset = 15
    Then N1 checks its leader-epoch-checkpoint
    And N1 determines that epoch 3 ended at offset 12
    And N1 responds with DivergingEpoch { epoch: 3, end_offset: 12 }
    And N2 truncates its log from offset 12 onward and sets fetch_offset to 12

  Scenario: No divergence — follower log is a prefix of leader log
    Given N1 is Leader with entries at offsets 0–10 (log end offset 11)
    And N2 has entries at offsets 0–7 (all matching the leader)
    When N2 sends a Fetch RPC with fetch_offset 8 (N2 has entries [0, 8))
    Then N1 responds with entries at offsets 8–10 (no DivergingEpoch)
    And N2 appends the new entries normally
```

---

## Feature: Check Quorum

```gherkin
Feature: Check Quorum
  As a Raft leader
  I need to periodically verify I can communicate with a majority of voters
  So that I step down if I am partitioned, preventing split-brain

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1
    And the Check Quorum interval is configured

  Scenario: Leader maintains quorum — all followers Fetching
    Given N2 and N3 are periodically sending Fetch RPCs to N1
    When the Check Quorum interval elapses
    Then N1 verifies that N2 and N3 have Fetched recently
    And N1 has quorum (N1 + N2 + N3 = 3 of 3)
    And N1 remains Leader

  Scenario: Leader loses quorum and steps down
    Given N2 and N3 are both unreachable (network partition)
    And neither N2 nor N3 has sent a Fetch RPC recently
    When the Check Quorum interval elapses
    Then N1 detects it can only reach itself (1 of 3 — not a majority)
    And N1 steps down to Follower state
    And N1 stops accepting client proposals

  Scenario: Leader has partial quorum (2 of 3) — continues
    Given N3 is unreachable but N2 is still Fetching
    When the Check Quorum interval elapses
    Then N1 verifies that N2 has Fetched recently (N1 + N2 = 2 of 3)
    And N1 maintains quorum and remains Leader

  Scenario: Check Quorum in 5-node cluster with 2 unreachable nodes
    Given a 5-node cluster [N1, N2, N3, N4, N5]
    And N1 is Leader
    And N4 and N5 are unreachable
    When the Check Quorum interval elapses
    Then N1 verifies that N2 and N3 are Fetching (N1 + N2 + N3 = 3 of 5)
    And N1 maintains quorum and remains Leader

  Scenario: Check Quorum in 5-node cluster with 3 unreachable nodes
    Given a 5-node cluster [N1, N2, N3, N4, N5]
    And N1 is Leader
    And N3, N4, and N5 are all unreachable
    When the Check Quorum interval elapses
    Then N1 can only confirm N2 (N1 + N2 = 2 of 5 — not a majority)
    And N1 steps down to Follower state
```

---

## Feature: Persistence and Crash Recovery

```gherkin
Feature: Persistence and Crash Recovery
  As a Raft node
  I need to durably persist currentTerm, votedFor, and the log before
  acknowledging any RPC
  So that the cluster can recover safely after node crashes

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And all persistence uses fsync to stable storage

  Scenario: Leader crash and recovery — follower catches up
    Given N1 is Leader for term 3 with committed entries at offsets 1–10
    When N1 crashes and restarts
    Then N1 reads its persisted currentTerm (3), votedFor, and log from disk
    And N1 starts as a Follower (does not assume leadership)
    And N1 waits for election timeout or Fetch responses from the new leader
    When a new leader is elected
    Then N1 Fetches any missing entries from the new leader

  Scenario: Node recovers votedFor correctly to prevent double voting
    Given N2 has voted for N1 in term 5 and persisted votedFor = N1
    When N2 crashes and restarts during term 5
    And N3 sends a Vote RPC to N2 for term 5
    Then N2 loads votedFor = N1 from disk
    And N2 rejects N3's vote because it already voted for N1 this term

  Scenario: Log entries survive node crash
    Given N2 has replicated entries at offsets 1–8 and persisted them to disk
    When N2 crashes and restarts
    Then N2's log contains entries at offsets 1–8 (recovered from disk)
    And N2 can resume Fetching from the leader at offset 8

  Scenario: Uncommitted entries from crashed leader are preserved
    Given N1 was Leader for term 2 and appended entry at offset 5 but it was not committed
    When N1 crashes and N3 becomes Leader for term 3
    And N3's log does not contain offset 5 from term 2
    And N1 restarts and Fetches from N3
    Then N1 receives DivergingEpoch and truncates offset 5
    And N1's log now matches N3's

  Scenario: All nodes crash and recover (full cluster restart)
    Given all three nodes have committed entries at offsets 1–10
    When all nodes crash simultaneously and restart
    Then each node recovers its currentTerm, votedFor, and log from stable storage
    And a new election occurs
    And the winning leader has all committed entries (offsets 1–10)
    And normal operation resumes with no data loss

  Scenario: fsync failure is treated as fatal
    Given N2 attempts to persist a log entry
    When the fsync call fails (I/O error)
    Then N2 does NOT acknowledge the entry
    And N2 reports the error and shuts down gracefully
    And the cluster continues with the remaining nodes
```

---

## Feature: Log Compaction and Snapshots

```gherkin
Feature: Log Compaction and Snapshots
  As a Raft node
  I need to periodically compact the log via snapshots
  So that disk usage remains bounded under sustained write load

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1

  Scenario: Node takes a snapshot and truncates the log prefix
    Given N1 has committed entries at offsets 0–100
    And N1's state machine has applied all entries up to offset 100
    When the snapshot threshold is reached (e.g., every 100 entries)
    Then N1 writes a snapshot to stable storage containing:
      | Field            | Value                       |
      | last_included_offset | 100                         |
      | last_included_term | 1                           |
      | voterSet         | [N1, N2, N3]                |
      | stateMachineData | <serialised state at offset 100> |
    And N1 performs prefix truncation of entries 0–100 via LogStore::truncate_prefix
    And N1's log start offset (LSO) advances to 101
    # Prefix truncation is a safe storage operation on committed entries
    # captured in the snapshot. It does NOT violate the append-only leader
    # invariant, which prohibits only suffix operations (overwriting entries
    # or deleting uncommitted tail entries).

  Scenario: Snapshot is consistent because committed logs are consistent
    Given N1, N2, and N3 have all committed entries at offsets 0–100
    When each node independently takes a snapshot at offset 100
    Then all three snapshots contain identical state machine data
    And each node performs prefix truncation on its own log independently

  Scenario: Snapshot includes voter set for recovery
    Given the current voter set is [N1, N2, N3]
    When N2 takes a snapshot
    Then the snapshot metadata includes the voter set [N1, N2, N3]
    And on crash recovery, N2 can reconstruct the voter configuration from the snapshot

  Scenario: New entries arrive after snapshot and prefix truncation
    Given N1 has taken a snapshot at offset 100 and performed prefix truncation of entries 0–100
    When a client proposes command "set y=2"
    Then N1 appends the entry at offset 101, term 1
    And the entry is replicated and committed normally
    And the log contains only entries from offset 101 onward
```

---

## Feature: Snapshot Transfer

```gherkin
Feature: Snapshot Transfer
  As a Raft leader
  I need to send a snapshot to a follower whose required log entries have
  been compacted away
  So that slow or newly joined nodes can catch up to the current state

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1

  Scenario: Follower receives snapshot when log entries are compacted
    Given N1 has committed entries at offsets 1–200
    And N1 has taken a snapshot at offset 150 and performed prefix truncation of entries 1–150
    And N1's log start offset (LSO) is 151
    And N2 has only replicated through offset 50 (N2's log end offset is 51)
    When N2 sends a Fetch RPC with fetch_offset 51
    Then N1 detects that fetch_offset 51 < LSO (151)
    And N1 responds with a SnapshotId field (offset 150, term 1)
    When N2 sends FetchSnapshot RPCs to download the snapshot
    Then N1 streams the snapshot in chunks
    And N2 reassembles the complete snapshot
    And N2 restores its state machine from the snapshot
    And N2 sets its log start offset to 151
    And N2 resumes Fetching entries from offset 151

  Scenario: Chunked snapshot transfer handles large state
    Given the snapshot at offset 150 is 10 MB
    And the chunk size is 1 MB
    When N2 sends FetchSnapshot RPCs
    Then N2 receives 10 chunks sequentially
    And each chunk includes an offset and a flag indicating whether it is the last
    And N2 reassembles all chunks into the full snapshot
    And N2 verifies the snapshot integrity before restoring

  Scenario: Snapshot transfer interrupted by leader change
    Given N2 is downloading a snapshot from N1 via FetchSnapshot
    And N2 has received 5 of 10 chunks
    When N1 crashes and N3 becomes the new Leader
    Then N2 aborts the in-progress snapshot transfer
    And N2 begins Fetching from N3
    And N3 may also respond with a SnapshotId if its log is compacted
    And N2 restarts the snapshot transfer from N3

  Scenario: Snapshot transfer for a newly joined observer
    Given N4 joins the cluster as an Observer with an empty log
    And N1's log starts at offset 201 (entries 1–200 compacted)
    When N4 sends its first Fetch RPC to N1 with fetch_offset 0
    Then N1 responds with SnapshotId (offset 200, term 1)
    And N4 downloads the snapshot via FetchSnapshot
    And N4 restores its state machine from the snapshot
    And N4 resumes Fetching entries from offset 201
```

---

## Feature: Cluster Bootstrap

```gherkin
Feature: Cluster Bootstrap
  As a cluster operator
  I need to form a new cluster from a static voter set
  So that the Raft cluster can begin operating with a known initial configuration

  # Covers tech spec §2.1.7 — Bootstrap & Recovery

  Background:
    Given three uninitialized nodes [N1, N2, N3]
    And a bootstrap configuration specifying voter set [N1, N2, N3]
    And each node is configured with the same cluster_id "cluster-abc-123"

  Scenario: Fresh cluster bootstrap with static voter set
    Given each node is configured with the initial voter set [N1, N2, N3]
    And each node is configured with cluster_id "cluster-abc-123"
    And each node has an empty log and no snapshot
    When all three nodes start simultaneously
    Then each node starts in Unattached state with term 0
    And each node loads the bootstrap voter set from its static configuration
    And each node transitions from Unattached to Follower state
    And a leader election occurs (one node's election timeout expires first)
    And the winning leader commits a VotersRecord control entry with [N1, N2, N3]
    And every RPC includes cluster_id "cluster-abc-123" for identity verification

  Scenario: Bootstrap leader appends initial VotersRecord
    Given N1 wins the initial election for term 1
    When N1 transitions to Leader
    Then N1 appends a LeaderChangeMessage (no-op) at offset 0, term 1
    And N1 appends a VotersRecord control entry at offset 1, term 1
      with voter set [N1, N2, N3]
    When both entries are committed (HW advances to 2; offsets 0–1 committed)
    Then the cluster is fully bootstrapped
    And subsequent membership changes use AddVoter / RemoveVoter RPCs

  Scenario: Node reads quorum-state file on startup
    Given N2 has previously participated in term 5 and voted for N1
    And N2's quorum-state file contains currentTerm=5, votedFor=N1
    When N2 restarts
    Then N2 reads its quorum-state file before processing any RPCs
    And N2's currentTerm is 5 and votedFor is N1
    And N2 does not vote for any other candidate in term 5

  Scenario: Uninitialized node joins as observer
    Given the cluster [N1, N2, N3] is running with committed entries 1–100
    And N4 is a new uninitialized node with no log and no snapshot
    When N4 starts (initially in Unattached state) and connects to the cluster
    Then N4 transitions to Follower state with Observer membership (non-voting)
    And N4 begins sending Fetch RPCs to the leader
    And if the leader's log starts beyond offset 0, N4 receives a SnapshotId
    And N4 downloads the snapshot via FetchSnapshot before normal replication

  Scenario: Bootstrap rejects mismatched cluster_id
    Given the cluster [N1, N2, N3] was bootstrapped with cluster_id "cluster-abc-123"
    And N4 is configured with a different cluster_id "cluster-xyz-999"
    When N4 attempts to send Fetch RPCs to N1
    Then N1 rejects all RPCs from N4 due to cluster_id mismatch in the RpcEnvelope
    And N4 cannot join the cluster
```

---

## Feature: Dynamic Quorum — Add Voter

```gherkin
Feature: Dynamic Quorum — Add Voter
  As a cluster operator
  I need to add a new voting member to the cluster one at a time
  So that the cluster can grow without risking disjoint majorities

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1

  Scenario: Add a fourth voter to the cluster
    Given N4 is an Observer that has caught up with the leader (log is current)
    When a client sends an AddVoter RPC for N4 to N1
    Then N1 appends a VotersRecord control entry to the log with voter set [N1, N2, N3, N4]
    When the VotersRecord is committed (replicated to a majority of the current voter set [N1, N2, N3])
    Then the active voter set becomes [N1, N2, N3, N4]
    And the quorum size increases to 3 (majority of 4)
    And N4 participates in future elections and quorum calculations

  Scenario: Only one voter change at a time
    Given an AddVoter RPC for N4 is in progress (VotersRecord not yet committed)
    When a client sends another AddVoter RPC for N5 to N1
    Then N1 rejects the second AddVoter with an error
    And the rejection message indicates a membership change is already in progress

  Scenario: AddVoter rejected when sent to non-leader
    When a client sends an AddVoter RPC for N4 to N2 (a follower)
    Then N2 rejects the request
    And N2 responds with the ID of the current leader (N1)

  Scenario: AddVoter during network partition
    Given N3 is partitioned from N1 and N2
    When a client sends AddVoter for N4 to N1
    Then N1 can still commit the VotersRecord with N1 + N2 = 2 of 3 (majority of current voters [N1, N2, N3])
    And N4 replicates the VotersRecord via Fetch as an observer
    And when the partition heals, N3 learns the new voter set via Fetch

  Scenario: Add voter fails if observer has not caught up
    Given N4 is an Observer whose log is significantly behind the leader
    When a client sends AddVoter for N4 to N1
    Then N1 rejects the AddVoter because N4 is not caught up
    And the rejection prevents an availability gap where N4's vote
      would be needed but N4 cannot meaningfully participate
```

---

## Feature: Dynamic Quorum — Remove Voter

```gherkin
Feature: Dynamic Quorum — Remove Voter
  As a cluster operator
  I need to remove a voting member from the cluster
  So that failed or decommissioned nodes do not affect quorum calculations

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1

  Scenario: Remove a follower from the cluster
    When a client sends a RemoveVoter RPC for N3 to N1
    Then N1 appends a VotersRecord with voter set [N1, N2]
    When the VotersRecord is committed (replicated to a majority of the NEW voter set [N1, N2])
    Then the new voter set [N1, N2] takes effect
    And the quorum size decreases to 2 (majority of 2)
    And N3 transitions to Unattached state once it learns of its removal via Fetch
    And N3's Vote RPCs are ignored by the remaining nodes

  Scenario: Remove the leader — leader steps down after commit
    When a client sends a RemoveVoter RPC for N1 (the leader itself) to N1
    Then N1 appends a VotersRecord with voter set [N2, N3]
    And N1 continues serving as leader until the VotersRecord is committed by the NEW voter set [N2, N3]
    When the VotersRecord is committed (majority of [N2, N3] have fetched it)
    Then the new voter set [N2, N3] takes effect
    And N1 steps down to Unattached state (no longer a member of the voter set)
    And N2 or N3 triggers a new election and becomes the new Leader

  Scenario: Removed node's vote requests are ignored
    Given N3 has been removed from the voter set
    And N3 has transitioned to Unattached after learning of its removal
    And a new leader (N2) is active with voter set [N1, N2]
    When N3 sends Vote RPCs (before it learns of its removal, or if it restarts without the VotersRecord)
    And N1 and N2 have recently heard from the leader
    Then N1 and N2 reject N3's Vote RPCs (Pre-Vote rejects; N3 not in voter set)
    And the cluster is not disrupted

  Scenario: UpdateVoter changes a node's endpoint address
    Given the voter set is [N1, N2, N3]
    And N3's endpoint changes from 10.0.0.3:9000 to 10.0.0.30:9000
    When a client sends an UpdateVoter RPC for N3 with the new endpoint to N1
    Then N1 appends a VotersRecord with updated endpoint for N3
    When the VotersRecord is committed (by the current voter set)
    Then all nodes update N3's address in their voter configuration
    And N3 remains a voting member with unchanged voting rights
```

---

## Feature: Dynamic Quorum — Observer Promotion

```gherkin
Feature: Dynamic Quorum — Observer Promotion
  As a new node joining the cluster
  I need to start as an Observer (non-voting) until my log is caught up
  So that I do not create an availability gap by being a voter
  with an empty or incomplete log

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1

  Scenario: New node joins as Observer and replicates log
    Given N4 joins the cluster as an Observer
    When N4 starts sending Fetch RPCs to N1
    Then N1 responds with log entries (or snapshot if log is compacted)
    And N4 replicates entries without participating in quorum
    And N4 does NOT vote in elections
    And N4's replication does NOT affect high watermark calculations

  Scenario: Observer is promoted to Voter after catching up
    Given N4 is an Observer and has replicated all entries up to the leader's log end
    When a client sends AddVoter for N4 to N1
    Then N1 accepts the request because N4 is caught up
    And N1 appends a VotersRecord with voter set [N1, N2, N3, N4]
    And once committed by the current voter set [N1, N2, N3], N4 becomes a full voting member

  Scenario: Observer survives leader election
    Given N4 is an Observer replicating from N1
    When N1 crashes and N2 becomes the new Leader for term 2
    Then N4 detects the leader change (via Fetch response or election)
    And N4 begins Fetching from N2
    And N4 continues replicating without disruption
```

---

## Feature: Client Interaction

```gherkin
Feature: Client Interaction
  As a client application embedding the xraft library
  I need to propose commands and read committed state
  So that I can build a replicated state machine on top of xraft

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1
    And each node runs a user-provided StateMachine implementation

  Scenario: Successful command proposal and commit — callback ordering
    When a client calls propose("set x=1") on the leader N1
    Then N1 stages the entry in the BatchAccumulator
    And the BatchAccumulator drains the entry to the log via IoAction::AppendLog
    And the propose future is parked in the DeferredCompletionQueue (keyed by offset)
    When followers Fetch and replicate the entry and the HW advances past it
    Then the event loop invokes the three-phase commit sequence in order:
      | Phase | Action                                                              |
      | 1     | StateMachine::apply("set x=1") — one call per committed command     |
      | 2     | Listener::handle_commit(batch) — one batch of committed AppRecords  |
      | 3     | DeferredCompletionQueue::complete — resolves the propose future     |
    And the propose future resolves with Ok (after phases 1–3 complete)
    And all callbacks are synchronous in-process calls within the event loop

  Scenario: Proposal rejected on follower
    When a client calls propose("set x=1") on N2 (a follower)
    Then N2 returns an error indicating it is not the leader
    And the error includes the current leader's identity (N1)

  Scenario: Proposal rejected on candidate during election
    Given no leader exists and N1 is a Candidate
    When a client calls propose("set x=1") on N1
    Then N1 returns an error indicating no leader is available

  Scenario: Read returns current committed state via log routing
    Given N1 is Leader for term 1 with high watermark 11 (offsets 0–10 committed)
    And the application state machine has applied all committed command entries
    When a client calls read() on N1
    Then read() routes through the log for safety (per tech spec §2.1.5)
    And the result contains the application's committed state machine state
    And the state reflects all applied command entries through offset 10
    And consensus metadata (term, role, HW, voter_set) is accessible separately via metrics
    And linearisable reads are out of scope for the initial implementation (per tech spec §2.2)

  Scenario: Read on a follower returns locally committed state
    Given N2 is a Follower with currentTerm 1 and local HW 9
    And the leader's HW is 11 (N2 has not yet received the latest HW)
    When a client calls read() on N2
    Then the result contains the state machine state through N2's local HW (offset 8 = last committed)
    And the state may lag behind the leader's because read() returns state based on local HW
    And linearisable reads are out of scope (per tech spec §2.2)

  Scenario: At-least-once semantics — duplicate proposal after leader failover
    Given a client proposes command "set x=1" to N1 (Leader)
    And N1 appends the entry and it is committed
    When N1 crashes before the client receives the commit acknowledgement
    And N2 becomes the new Leader for term 2
    And the client retries proposing "set x=1" to N2
    Then N2 appends a second "set x=1" entry to the log
    And the duplicate is committed and applied
    And xraft does NOT perform built-in deduplication
    And it is the application's responsibility to make commands idempotent

  Scenario: Application-level dedup via request IDs (out of xraft scope)
    Given the application wraps commands with unique request IDs
    And the state machine's apply() method tracks processed request IDs
    When a duplicate command arrives with an already-processed request ID
    Then the state machine ignores the duplicate during apply()
    And xraft treats it as a normal committed entry (no special handling)

  Scenario: Listener callbacks on leader change
    Given the application has registered a Listener with handle_leader_change
    When a new leader is elected (N2 for term 2)
    Then on N2, the event loop invokes Listener::handle_leader_change(leader_id=N2, term=2)
    And on N1, the event loop invokes Listener::handle_leader_change(leader_id=N2, term=2)
    And the callback is invoked synchronously within the event loop before IoActions are dispatched

  Scenario: Listener callbacks on commit — three-phase ordering
    Given the application has registered a Listener with handle_commit
    And the application implements StateMachine with apply()
    When entries at offsets 5–7 are committed (HW advances past 7)
    Then the event loop executes the three-phase commit sequence per the architecture doc:
    And Phase 1: StateMachine::apply is called once per committed command entry (offsets 5, 6, 7 in order)
    And control records (if any) are handled internally and never reach StateMachine::apply
    And Phase 2: Listener::handle_commit receives one batch of committed AppRecords [5, 6, 7]
    And Phase 3: DeferredCompletionQueue::complete resolves client futures for offsets 5–7
    And all three phases are synchronous in-process calls within the event loop
    And the Fetch response (via IoStage) reflects the same HW that the callbacks observed
```

---

## Feature: Safety Invariants

```gherkin
Feature: Safety Invariants
  As a Raft implementation
  I must enforce the five Raft safety invariants at all times
  So that the replicated state machine is correct under all conditions

  # These scenarios verify the invariants defined in the tech spec §2.1.1.
  # They are designed for deterministic simulation with fault injection.

  Scenario: Leader Election Safety — at most one leader per term
    Given a 5-node cluster [N1, N2, N3, N4, N5]
    And a network partition splits the cluster into [N1, N2] and [N3, N4, N5]
    When N3 starts an election for term 2 in partition [N3, N4, N5]
    And N3 wins with votes from N4 and N5
    Then N3 is the only leader for term 2
    And N1 cannot win an election for term 2 in partition [N1, N2]
      because N1 can only get 2 of 5 votes (not a majority)
    And at no point do two leaders exist for the same term

  Scenario: Append-Only Leader — leader never overwrites or suffix-truncates its own log
    Given N1 is Leader for term 3
    And N1's log contains entries at offsets 1–10
    When N1 appends a new entry at offset 11
    Then entries at offsets 1–10 are unmodified in content and position
    And N1 never overwrites an existing entry with different content
    And N1 never performs suffix truncation (only followers truncate on divergence)
    # Note: prefix truncation (compaction) of committed entries after
    # snapshotting is a separate, safe storage operation (see Feature:
    # Log Compaction and Snapshots) and does NOT violate this invariant.
    # The append-only property applies to suffix operations — the leader
    # only appends new entries at the end and never modifies prior entries.

  Scenario: Leader Completeness — new leader has all committed entries
    Given N1 was Leader and committed entries at offsets 1–20
    When N1 crashes and N2 wins election for term 2
    Then N2's log contains all committed entries from offsets 1–20
    And no committed entry is ever lost during leadership transitions

  Scenario: Log Matching — same offset and term implies identical logs up to that point
    Given N1 is Leader and has committed entries at offsets 1–15
    And N2 and N3 have replicated all entries
    When N2's entry at offset 10 has the same term as N3's entry at offset 10
    Then N2 and N3's logs are identical for all entries at offsets 1–10

  Scenario: State Machine Safety — no two nodes apply different entries at same offset
    Given entries at offsets 1–10 are committed and applied across all nodes
    Then for every offset 1–10, all nodes have applied the same entry
    And no node ever applies a different entry at any previously applied offset

  Scenario: Invariants hold under adversarial conditions (simulation)
    Given a 5-node cluster under deterministic simulation
    And fault injection is enabled:
      | Fault Type           | Configuration                      |
      | Network partition    | Random partitions every 500ms      |
      | Message delay        | 0–100ms uniform random delay       |
      | Message reorder      | Enabled                            |
      | Node crash           | Random crash/restart every 2s      |
      | fsync delay          | 1–50ms                             |
    When 1000 client proposals are submitted over a 30-second simulated period
    Then all committed proposals are applied in the same order on all nodes
    And no committed entry is lost
    And at most one leader exists per term at any point
    And no node applies different entries at the same log offset
    And the simulation completes without assertion failures
```

---

## Feature: Observability and Metrics

```gherkin
Feature: Observability and Metrics
  As a cluster operator
  I need access to key Raft metrics
  So that I can monitor cluster health and diagnose issues

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 2

  Scenario: Current leader metric is accurate
    When querying metrics on any node
    Then the "current-leader" metric returns N1's node ID
    And the "current-epoch" metric returns 2

  Scenario: Leader metric updates after election
    Given N1 crashes
    And N2 wins election for term 3
    When querying metrics on N2
    Then "current-leader" returns N2's node ID
    And "current-epoch" returns 3
    When querying metrics on N3
    Then "current-leader" returns N2's node ID
    And "current-epoch" returns 3

  Scenario: Unknown leader metric during election
    Given no leader exists (election in progress)
    When querying metrics on any node
    Then "current-leader" returns -1 (unknown)

  Scenario: Election latency metric is recorded
    Given a leader election completes in 45ms
    When querying metrics on the new leader
    Then "election-latency-avg" reflects the 45ms election duration

  Scenario: Append rate metric tracks leader throughput
    Given the leader N1 appends 500 entries over 10 seconds
    When querying metrics on N1
    Then "append-records-rate" is approximately 50 records/second

  Scenario: Commit latency metric tracks end-to-end commit time
    Given a client proposes a command
    And the command is committed after 12ms (propose to HW advancement)
    When querying metrics on the leader
    Then "commit-latency-avg" reflects the 12ms latency
```

---

## Appendix: Scenario Coverage Matrix

| Tech Spec Section | Feature Covered | Scenario Count |
|-------------------|-----------------|----------------|
| §2.1.1 Node roles / Leader election | Leader Election | 9 |
| §2.1.1 Pre-Vote protocol | Pre-Vote Protocol | 5 |
| §2.1.1 Log replication (pull-based) | Pull-Based Log Replication | 7 |
| §2.1.1 High watermark (HW) | High Watermark Advancement | 5 |
| §2.1.1 Safety invariants | Safety Invariants | 6 |
| §2.1.1 No-op commit on leader start | High Watermark Advancement (scenario 5) | — |
| §2.1.1 Check Quorum | Check Quorum | 5 |
| §2.1.1 Persistence | Persistence and Crash Recovery | 6 |
| §2.1.2 Snapshotting | Log Compaction and Snapshots | 4 |
| §2.1.2 Snapshot transfer | Snapshot Transfer | 4 |
| §2.1.2 Log truncation / Divergence | Log Divergence and Truncation | 4 |
| §2.1.3 Single-node changes | Add Voter / Remove Voter / UpdateVoter | 9 |
| §2.1.3 Non-voting members (observers) | Observer Promotion | 3 |
| §2.1.3 Leader step-down | Remove Voter (scenario 2) | — |
| §2.1.4 Identity & fencing | Pull-Based Log Replication (scenarios 5–7) | — |
| §2.1.4 Divergence detection | Log Divergence and Truncation | — |
| §2.1.5 Library API | Client Interaction | 9 |
| §2.1.6 Metrics | Observability and Metrics | 6 |
| §2.1.6 Deterministic simulation | Safety Invariants (scenario 6) | — |
| §2.1.7 Bootstrap & recovery | Cluster Bootstrap | 5 |
| **Total** | **16 Features** | **87 Scenarios** |
