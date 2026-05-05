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
14. [Feature: Identity and Fencing](#feature-identity-and-fencing)
15. [Feature: Client Interaction](#feature-client-interaction)
16. [Feature: Safety Invariants](#feature-safety-invariants)
17. [Feature: Observability and Metrics](#feature-observability-and-metrics)
18. [Feature: Graceful Shutdown and Lifecycle](#feature-graceful-shutdown-and-lifecycle)
19. [Feature: Error Recovery and Fault Tolerance](#feature-error-recovery-and-fault-tolerance)
20. [Feature: Log Integrity and CRC Recovery](#feature-log-integrity-and-crc-recovery)
21. [Feature: Single-Node Cluster](#feature-single-node-cluster)
22. [Feature: Batch Accumulation and Group Commit](#feature-batch-accumulation-and-group-commit)
23. [Feature: Stale and Delayed Message Handling](#feature-stale-and-delayed-message-handling)
24. [Feature: Sequential Membership Changes](#feature-sequential-membership-changes)

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
    Then N1 appends "set x=1" at offset 1, term 1 to its log (log_end_offset becomes 2)
    When N2 sends a Fetch RPC to N1 with fetch_offset 1 (N2 has entries [0, 1))
    Then N1 responds with entry [offset 1, term 1, "set x=1"] (entries starting at fetch_offset)
    And N2 appends the entry to its local log (N2's log_end_offset becomes 2)
    When N3 sends a Fetch RPC to N1 with fetch_offset 1 (N3 has entries [0, 1))
    Then N1 responds with entry [offset 1, term 1, "set x=1"]
    And N3 appends the entry to its local log

  Scenario: Batch replication — multiple entries in one Fetch
    Given the leader N1 has application entries at offsets 1–5 (offset 0 is LeaderChangeMessage; log_end_offset = 6)
    And N2 has only replicated offset 0 (N2's log_end_offset = 1)
    When N2 sends a Fetch RPC to N1 with fetch_offset 1 (N2 has entries [0, 1))
    Then N1 responds with entries at offsets [1, 2, 3, 4, 5]
    And N2 appends all 5 entries to its local log in order (N2's log_end_offset becomes 6)

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
    Then N1 consults its leader-epoch checkpoint for epoch 1
    And N1 finds that epoch 1 ended at offset 15 (term 3 starts at 15)
    And N2's fetch_offset (20) exceeds the epoch 1 boundary (15) → divergence detected
    And N1 responds with DivergingEpoch { epoch: 1, end_offset: 15 }
    And N2 truncates its log from offset 15 onward (via LogStore::truncate_suffix(15))
    And N2 sets its fetch_offset to 15 (log_end_offset is now 15)
    And N2 resumes Fetching with last_fetched_epoch = 1 and fetch_offset = 15

  Scenario: Fetch includes cluster_id for identity verification
    Given the cluster is configured with cluster_id "cluster-abc-123"
    When N2 sends a Fetch RPC with a mismatched cluster_id "cluster-xyz-999" in the RpcEnvelope
    Then N1 rejects the Fetch due to cluster_id mismatch (per architecture §6.2)
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
    Given N1 was Leader for term 1 and appended entries at offsets 0–5
    And N2 replicated entries 0–4 from N1 (N2's log_end_offset is 5, last entry term 1)
    And N1 crashed before entry at offset 4 was committed
    And N3 becomes Leader for term 2 and appends new entries at offsets 4–6
    And N3's leader-epoch-checkpoint maps term 1 → start 0, term 2 → start 4
    When N2 sends a Fetch RPC to N3 with last_fetched_epoch=1 and fetch_offset=5
    Then N3 checks its leader-epoch-checkpoint for epoch 1
    And N3 finds epoch 1 ended at offset 4 (term 2 starts at 4)
    And N2's fetch_offset (5) > epoch 1 end (4) → divergence detected
    And N3 responds with DivergingEpoch { epoch: 1, end_offset: 4 }
    And N2 truncates its log from offset 4 onward (via LogStore::truncate_suffix(4))
    When N2 sends another Fetch RPC with last_fetched_epoch=1 and fetch_offset=4
    Then N3 responds with entries at offsets 4–6 from term 2
    And N2 appends the leader's entries and its log now matches N3's

  Scenario: Multiple Fetch rounds to resolve deep divergence
    Given N2 has entries at offsets 0–10 from terms [1,1,1,1,2,2,2,3,3,3,3]
    And the new leader N3 has entries at offsets 0–8 from terms [1,1,1,1,2,2,2,4,4]
    And N3's leader-epoch-checkpoint maps term 1→0, term 2→4, term 4→7
    When N2 sends a Fetch RPC to N3 with last_fetched_epoch=3 and fetch_offset=11
    Then N3 has no entry for epoch 3 in its checkpoint (epoch 3 never existed on leader)
    And N3 finds the next lower epoch: epoch 2 ended at offset 7 (term 4 starts at 7)
    And N3 responds with DivergingEpoch { epoch: 3, end_offset: 7 }
    And N2 truncates its log from offset 7 onward (discards offsets 7–10)
    When N2 sends another Fetch RPC with last_fetched_epoch=2 and fetch_offset=7
    Then N3 checks epoch 2: it ended at offset 7 on the leader; N2's fetch_offset is 7 — no divergence
    And N3 responds with entries at offsets 7–8 from term 4
    And N2 appends entries 7–8 and its log now matches the leader's

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

  Scenario: Leader crash and recovery — HW rebuilt from snapshot
    Given N1 is Leader for term 3 with committed entries at offsets 0–10 (HW = 11)
    And N1 has a snapshot at offset 8 (last_included_offset = 8, last_included_term = 3)
    When N1 crashes and restarts
    Then N1 reads its quorum-state file: currentTerm = 3, votedFor from disk
    And N1 loads the latest snapshot (last_included_offset = 8)
    And N1 restores StateMachine from the snapshot's AppSnapshot
    And N1 sets HW = 9 (snapshot.last_included_offset + 1; entries [0, 9) known committed)
    And N1 scans log entries from offset 9 onward but does NOT apply them (committed status unknown)
    And N1 resumes as Follower (never assumes leadership — must re-win election)
    When a new leader is elected
    Then N1 Fetches from the new leader and receives the authoritative HW
    And N1 applies committed entries from offset 9 onward as HW advances via Fetch responses

  Scenario: Recovery without snapshot — HW starts at 0
    Given N2 has replicated entries at offsets 0–8 and persisted them to disk
    And N2 has no snapshot
    When N2 crashes and restarts
    Then N2 reads its quorum-state file for currentTerm and votedFor
    And N2 sets HW = 0 (no snapshot → HW initialised to 0; no entries known committed)
    And N2 scans log entries 0–8 but does NOT apply them to StateMachine
    And N2 resumes as Follower and starts election timer
    When N2 receives Fetch responses from the leader with HW
    Then N2 advances its local HW and applies committed entries to StateMachine

  Scenario: Node recovers votedFor correctly to prevent double voting
    Given N2 has voted for N1 in term 5 and persisted votedFor = N1 in quorum-state file
    When N2 crashes and restarts during term 5
    And N3 sends a Vote RPC to N2 for term 5
    Then N2 loads quorum-state: currentTerm = 5, votedFor = N1
    And N2 rejects N3's vote because it already voted for N1 this term

  Scenario: Uncommitted entries from crashed leader are preserved until divergence
    Given N1 was Leader for term 2 and appended entry at offset 5 but it was not committed
    When N1 crashes and N3 becomes Leader for term 3
    And N3's log does not contain offset 5 from term 2
    And N1 restarts and Fetches from N3
    Then N1 receives DivergingEpoch and truncates offset 5
    And N1's log now matches N3's

  Scenario: All nodes crash and recover (full cluster restart)
    Given all three nodes have committed entries at offsets 0–10 (HW was 11)
    And each node has a snapshot at offset 10
    When all nodes crash simultaneously and restart
    Then each node reads its quorum-state file for currentTerm and votedFor
    And each node loads its snapshot (last_included_offset = 10), setting HW = 11
    And each node restores its StateMachine from the snapshot's AppSnapshot
    And each node resumes as Follower (none assumes leadership)
    And a new election occurs (election timeouts fire)
    And the winning leader has all committed entries (its log includes offsets 0–10)
    And normal operation resumes with no data loss

  Scenario: fsync failure is treated as fatal
    Given N2 attempts to persist a log entry
    When the fsync call fails (I/O error)
    Then N2 does NOT acknowledge the entry
    And N2 reports the error and shuts down gracefully
    And the cluster continues with the remaining nodes

  Scenario: Recovery with pending uncommitted VotersRecord in log
    Given N1 was Leader for term 3 with voter set [N1, N2, N3]
    And N1 appended a VotersRecord [N1, N2, N3, N4] at offset 20 that was NOT committed (HW < 21)
    And N1 has a snapshot at offset 10 with voters [N1, N2, N3]
    When N1 crashes and restarts
    Then N1 reads its quorum-state file: currentTerm = 3
    And N1 loads the latest snapshot: committed voter set = [N1, N2, N3]
    And N1 scans the log from offset 11 onward (metadata only — no SM replay)
    And N1 finds the uncommitted VotersRecord at offset 20 and stores it as pending_membership_change
    And N1 does NOT replace the committed voter set [N1, N2, N3] with [N1, N2, N3, N4]
    And election quorum uses the committed voter set [N1, N2, N3]
    And HW advancement for entries at offset ≥ 20 uses the pending voter set [N1, N2, N3, N4]
    And N1 resumes as Follower and awaits a leader to learn the authoritative HW

  Scenario: Leader-epoch checkpoint rebuilt from log and snapshot on recovery
    # Per architecture §2.2: LeaderEpochCheckpoint is loaded into memory on
    # startup. The checkpoint maps each leader epoch to its start offset and
    # is rebuilt from the snapshot metadata plus any LeaderChangeMessage entries
    # in the recovered log (it is an in-memory cache, not a persisted file
    # separate from the log).
    Given N1 has a snapshot at offset 50 with last_included_term = 3
    And N1's log after the snapshot contains:
      | Offset | Term | EntryType            |
      | 51     | 3    | Command              |
      | 52     | 4    | LeaderChangeMessage  |
      | 53     | 4    | Command              |
    When N1 crashes and restarts
    Then N1 rebuilds the leader-epoch checkpoint from:
      | Epoch | Start Offset | Source             |
      | 3     | ≤ 50         | Snapshot metadata  |
      | 4     | 52           | Log scan           |
    And the leader-epoch checkpoint is available in memory for Fetch validation
    And N1 can detect divergence correctly when serving as leader

  Scenario: Corrupt quorum-state file on recovery triggers crash-stop
    # Per architecture §6.3 and risk R7: if the quorum-state file is
    # corrupt or partially written (e.g., torn write during fsync), the
    # node cannot safely determine its votedFor — operating without
    # accurate voting state risks double-voting and violates election
    # safety. The node must fail closed.
    Given N2 has a quorum-state file that is corrupt (partial write or bad checksum)
    When N2 attempts to start
    Then N2 detects the corruption during QuorumStateStore::load()
    And N2 fails to start with an irrecoverable error
    And N2 does NOT participate in any election or accept any RPC
    And the corruption must be resolved externally before N2 can restart
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
    Then N1 writes a snapshot to stable storage with SnapshotMetadata and AppSnapshot:
      | Field                        | Value                                              |
      | metadata.last_included_offset | 100                                               |
      | metadata.last_included_term   | 1                                                 |
      | metadata.voters              | [N1, N2, N3]                                       |
      | app_snapshot.data            | StateMachine::snapshot() return value (opaque bytes)|
    And N1 performs prefix truncation of entries 0–100 via LogStore::truncate_prefix
    And N1's log start offset (LSO) advances to 101
    # Prefix truncation is a safe storage operation on committed entries
    # captured in the snapshot. It does NOT violate the append-only leader
    # invariant, which prohibits only suffix operations (overwriting entries
    # or deleting uncommitted tail entries).

  Scenario: Snapshot is consistent because committed logs are consistent
    Given N1, N2, and N3 have all committed entries at offsets 0–100 (HW = 101)
    And each node's StateMachine has applied the same command entries in the same order
    When each node independently takes a snapshot at offset 100
    Then each Snapshot contains:
      | Part                          | Assertion                                           |
      | metadata.last_included_offset | 100                                                 |
      | metadata.last_included_term   | same term on all three nodes                        |
      | metadata.voters               | [N1, N2, N3]                                        |
      | app_snapshot.data             | equal across all three (StateMachine::snapshot() returns identical bytes) |
    And each node performs prefix truncation on its own log independently

  Scenario: Snapshot includes voter set for recovery
    Given the current voter set is [N1, N2, N3]
    And N2 has committed entries through offset 50 (HW = 51)
    When N2 takes a snapshot at offset 50
    Then the Snapshot.metadata.voters is [N1, N2, N3]
    And the Snapshot.metadata.last_included_offset is 50
    And the Snapshot.metadata.last_included_term matches the term of the entry at offset 50
    And on crash recovery, N2 restores the voter set from Snapshot.metadata.voters

  Scenario: New entries arrive after snapshot and prefix truncation
    Given N1 has taken a snapshot at offset 100 and performed prefix truncation of entries 0–100
    When a client proposes command "set y=2"
    Then N1 appends the entry at offset 101, term 1
    And the entry is replicated and committed normally
    And the log contains only entries from offset 101 onward

  Scenario: Snapshot taken with uncommitted VotersRecord uses committed voter set
    # Per architecture §3.2: VotersRecord is a control record. The voter
    # set stored in snapshot metadata is always the COMMITTED voter set,
    # not the pending one. Snapshots capture committed state only.
    Given N1 is Leader with committed voter set [N1, N2, N3]
    And N1 has appended a VotersRecord [N1, N2, N3, N4] at offset 90 (not yet committed; HW < 91)
    And N1 has committed entries through offset 80 (HW = 81)
    When N1 takes a snapshot at offset 80
    Then the Snapshot.metadata.voters is [N1, N2, N3] (the committed voter set)
    And the Snapshot.metadata does NOT include the pending voter set [N1, N2, N3, N4]
    And the uncommitted VotersRecord at offset 90 remains in the log (not compacted — only entries ≤ 80 are snapshotted)
    And on recovery from this snapshot, the committed voter set is correctly [N1, N2, N3]
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
    Given N1 has committed entries at offsets 0–200 (HW = 201)
    And N1 has taken a snapshot at offset 150 and performed prefix truncation
    And N1's log start offset (LSO) is 151
    And N2 has only replicated through offset 50 (N2's fetch_offset is 51, meaning entries [0, 51))
    When N2 sends a Fetch RPC with fetch_offset 51
    Then N1 detects that fetch_offset (51) < LSO (151)
    And N1 responds with entries=[] and snapshot_id = { offset: 150, epoch: 1 }
    When N2 sends FetchSnapshot RPCs to download the snapshot at position 0
    Then N1 streams the snapshot via SnapshotIO::read_chunk
    And N2 receives chunks until is_last_chunk = true
    And N2 performs atomic snapshot install:
      | Step | Action                                                    |
      | 1    | StateMachine::restore(app_snapshot) — restores SM state   |
      | 2    | Set log_start_offset to 151                               |
      | 3    | Update voter set from SnapshotMetadata.voters             |
      | 4    | fsync all state                                           |
    And N2 resumes Fetching from offset 151 with updated fetch_offset = 151

  Scenario: Chunked snapshot transfer handles large state
    Given N1's snapshot at offset 150 is 10 MB
    And SnapshotIO::read_chunk uses a max_bytes of 1 MB per chunk
    When N2 sends FetchSnapshot RPCs with incrementing positions
    Then N2 receives chunks: (position=0, is_last_chunk=false),
      (position=1048576, is_last_chunk=false), ... (position=9437184, is_last_chunk=true)
    And N2 writes each chunk via SnapshotWriter (from SnapshotIO::begin_receive)
    And N2 verifies the snapshot integrity (e.g., checksum) before calling StateMachine::restore

  Scenario: Snapshot transfer interrupted by leader change
    Given N2 is downloading a snapshot from N1 via FetchSnapshot
    And N2 has received 5 of 10 chunks
    When N1 crashes and N3 becomes the new Leader
    Then N2 aborts the in-progress snapshot transfer
    And N2 begins Fetching from N3
    And N3 may also respond with a SnapshotId if its log is compacted
    And N2 restarts the snapshot transfer from N3

  Scenario: Snapshot transfer for a newly joined observer
    Given N4 joins the cluster as an Observer with an empty log (log_end_offset = 0)
    And N1's LSO is 201 (entries 0–200 compacted into a snapshot at offset 200)
    When N4 sends its first Fetch RPC to N1 with fetch_offset 0
    Then N1 detects fetch_offset (0) < LSO (201)
    And N1 responds with snapshot_id = { offset: 200, epoch: 1 }
    And N4 downloads the snapshot via FetchSnapshot, chunk by chunk
    And N4 calls StateMachine::restore(app_snapshot) to restore its state
    And N4 sets its log_start_offset to 201
    And N4 resumes Fetching entries from offset 201

  Scenario: Listener receives handle_load_snapshot callback after snapshot install
    Given N2 falls behind and receives a SnapshotId from the leader in a Fetch response
    And N2 downloads the snapshot via FetchSnapshot and completes the transfer
    When N2 installs the snapshot (StateMachine::restore succeeds)
    Then the event loop invokes Listener::handle_load_snapshot(reader) on N2
    And the SnapshotReader provides access to the snapshot's AppSnapshot payload
    And the application can use this callback to rebuild read-side state
    And handle_load_snapshot is called synchronously within the event loop
    And after the callback, N2 resumes Fetching from the leader normally
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
    And N1's log_end_offset is 2
    # Two-round visibility: followers must Fetch and then report back
    When N2 sends a Fetch RPC with fetch_offset 0
    Then N1 responds with entries [0, 1] and current HW
    When N2 sends a Fetch RPC with fetch_offset 2 (N2 now has entries [0, 2))
    Then N1 records N2's fetch_offset as 2
    And N1 recalculates HW: sorted desc [N1=2, N2=2, N3=0] → index 1 → 2
    And N1 advances HW to 2 (offsets 0 and 1 committed: 0 < 2 ✓, 1 < 2 ✓)
    And the VotersRecord at offset 1 is committed
    And the cluster is fully bootstrapped
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
    # Per architecture doc §5.5: once a VotersRecord is appended, HW advancement
    # immediately uses the NEW voter set for quorum calculation. N4's fetch_offset
    # counts toward commit of the VotersRecord itself.
    Given N4 is an Observer that has caught up with the leader (N4's fetch_offset ≥ leader's HW)
    When a client sends an AddVoter RPC for N4 to N1
    Then N1 validates N4 is caught up (fetch_offset ≥ HW) and no pending VotersRecord exists
    And N1 appends a VotersRecord control entry to the log with voter set [N1, N2, N3, N4]
    When the VotersRecord is committed (majority of the NEW voter set [N1, N2, N3, N4] — i.e. 3 of 4)
    Then the active voter set becomes [N1, N2, N3, N4]
    And the quorum size increases to 3 (majority of 4)
    And N4 participates in future elections and HW advancement calculations

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
    # The NEW voter set [N1, N2, N3, N4] is used for quorum (majority = 3 of 4).
    # N4's fetch_offset counts toward commit (architecture §5.5 Quorum transition).
    Given N3 is partitioned from N1 and N2
    And N4 is an Observer that has caught up (fetch_offset ≥ HW)
    When a client sends AddVoter for N4 to N1
    Then N1 appends a VotersRecord [N1, N2, N3, N4]
    And N4 replicates the VotersRecord via Fetch as an observer-becoming-voter
    And N1 can commit the VotersRecord with N1 + N2 + N4 = 3 of 4 (majority of NEW set [N1, N2, N3, N4])
    And N3's fetch_offset is irrelevant — 3 of 4 already achieved without N3
    And when the partition heals, N3 learns the new voter set via Fetch

  Scenario: Add voter fails if observer has not caught up
    # Per architecture §5.5: leader checks observer's fetch_offset against HW.
    Given N4 is an Observer whose fetch_offset is 50 and the leader's HW is 100
    When a client sends AddVoter for N4 to N1
    Then N1 checks N4's tracked fetch_offset (50) against its own HW (100)
    And N1 rejects the AddVoter with MembershipError::NodeNotCaughtUp (fetch_offset < HW)
    And the rejection prevents an availability gap where N4's vote
      would be needed but N4 has a stale log and cannot quickly participate

  Scenario: Uncommitted AddVoter VotersRecord lost on leader crash
    Given a client sends AddVoter for N4 to N1
    And N1 appends a VotersRecord [N1, N2, N3, N4] at offset 20
    And the VotersRecord has NOT yet been committed (HW < 21)
    When N1 crashes before a majority replicates the VotersRecord
    And N2 wins election for term 2
    Then N2's log may or may not contain the uncommitted VotersRecord
    And if present, N2 does not treat it as effective (uncommitted VotersRecords do not change the active voter set for elections)
    And election quorum continues using the last committed voter set [N1, N2, N3]
    And if N2 becomes leader and the uncommitted VotersRecord survives in its log,
      HW advancement for entries at or after the VotersRecord's offset still uses
      the new voter set [N1, N2, N3, N4] per architecture §5.5 quorum transition —
      but the VotersRecord may be truncated during divergence handling if it conflicts
      with the new leader's log
    And the membership change must be re-submitted to the new leader

  Scenario: Single-change invariant holds after failover with uncommitted VotersRecord
    Given an AddVoter for N4 was in progress on N1 (uncommitted VotersRecord in log)
    And N1 crashes and N2 becomes Leader for term 2
    And N2's log still contains the uncommitted VotersRecord from term 1
    When a client sends another AddVoter for N5 to N2
    Then N2 rejects the request because an uncommitted VotersRecord exists in its log
    And the single-change invariant (architecture §5.5) is enforced even across leader transitions
    And the original VotersRecord must be committed or truncated before a new change is accepted

  Scenario: AddVoter commits even when new node becomes unreachable (3 of 4 majority)
    # Because HW advancement uses the NEW voter set [N1, N2, N3, N4],
    # all four nodes' fetch_offsets count toward quorum. However, if
    # N4 becomes unreachable, the remaining 3 nodes (N1 + N2 + N3)
    # still form a majority of the 4-node set (3 of 4 ≥ ⌊4/2⌋ + 1 = 3).
    Given N4 is an Observer that was caught up (fetch_offset ≥ HW)
    When a client sends AddVoter for N4 to N1
    And N1 appends a VotersRecord [N1, N2, N3, N4]
    And N4 becomes unreachable immediately after the VotersRecord is appended
    Then N1, N2, N3 have the VotersRecord (3 of 4 new voters — a majority)
    And the VotersRecord can still commit because 3 of 4 is a majority of the NEW set
    And N4 will learn the new config when the partition heals and it resumes Fetching
```

---

## Feature: Dynamic Quorum — Remove Voter

```gherkin
Feature: Dynamic Quorum — Remove Voter
  As a cluster operator
  I need to remove a voting member from the cluster
  So that failed or decommissioned nodes do not affect HW advancement or elections

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
    When the VotersRecord is committed (majority of the NEW voter set — same members [N1, N2, N3], 2 of 3)
    Then all nodes update N3's address in their voter configuration
    And N3 remains a voting member with unchanged voting rights

  Scenario: Removed node transitions to Unattached after learning removal
    Given N3 has been removed via a committed VotersRecord [N1, N2]
    When N3 fetches and applies the VotersRecord that excludes it
    Then N3 transitions to Unattached state (per architecture §5.6)
    And N3 stops participating in elections
    And N3 does not send Vote RPCs
    And N3 is no longer counted for HW advancement or election quorum by any node

  Scenario: RemoveVoter commits with the new voter set for quorum
    When a client sends RemoveVoter for N3 to N1
    And N1 appends a VotersRecord with voter set [N1, N2]
    Then HW advancement for this VotersRecord requires a majority of the NEW voter set [N1, N2]
    And N3's fetch_offset is NOT counted toward HW advancement for this record
    And once both N1 and N2 have the VotersRecord at or past their fetch_offset, HW advances
    And the VotersRecord is committed and N3 is removed

  Scenario: Quorum transition — HW uses new voter set on append, elections on commit
    # Per architecture §5.5/§3.2: HW advancement switches to the new voter set
    # immediately when the VotersRecord is appended. Election quorum (the active
    # voter set for voting) only switches when the VotersRecord is committed.
    Given N4 is an Observer caught up with the leader (fetch_offset ≥ HW)
    When a client sends AddVoter for N4 to N1 and N1 appends VotersRecord [N1, N2, N3, N4]
    Then HW advancement for entries at or after the VotersRecord's offset immediately uses
      the new voter set [N1, N2, N3, N4] — N4's fetch_offset counts toward commit
    And election quorum (e.g., if an election starts before the VotersRecord commits)
      still uses the last committed voter set [N1, N2, N3]
    When the VotersRecord is committed (majority of [N1, N2, N3, N4] = 3 of 4)
    Then the active voter set for elections becomes [N1, N2, N3, N4]
    And both HW advancement and election quorum now use [N1, N2, N3, N4]
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
    And N4's fetch_offset ≥ leader's current HW (caught-up threshold met)
    When a client sends AddVoter for N4 to N1
    Then N1 accepts the request because N4 is caught up (fetch_offset ≥ HW)
    And N1 appends a VotersRecord with voter set [N1, N2, N3, N4]
    And once committed by a majority of the NEW voter set [N1, N2, N3, N4] (3 of 4), N4 becomes a full voting member

  Scenario: Observer survives leader election
    Given N4 is an Observer replicating from N1
    When N1 crashes and N2 becomes the new Leader for term 2
    Then N4 detects the leader change (via Fetch response or election)
    And N4 begins Fetching from N2
    And N4 continues replicating without disruption

  Scenario: Observer election timeout does not disrupt the cluster
    # An observer's election timeout may expire if it has not heard from
    # the leader. However, because observers are not in the voter set,
    # their Vote RPCs (if sent) cannot win a majority among voters. The
    # implementation should suppress election starts for observer nodes
    # entirely, since they cannot form a valid quorum.
    Given N4 is an Observer in a 3-voter cluster [N1, N2, N3]
    And N1 is Leader for term 1
    When N4 cannot reach N1 (network fault) and its election timeout expires
    Then N4 does NOT transition to Candidate state
    And N4 does NOT send Vote RPCs
    And the cluster election state is unaffected
    And N4 retries Fetch RPCs to the leader after the timeout
```

---

## Feature: Identity and Fencing

```gherkin
Feature: Identity and Fencing
  As a Raft cluster
  I need every RPC envelope to carry cluster_id and leader_epoch
  So that cross-cluster contamination and stale-leader messages are rejected
  # Per architecture doc §6.2 and tech spec §2.1.4:
  # Every RpcEnvelope carries { cluster_id, leader_epoch, source, payload }.
  # Receivers reject messages with mismatched cluster_id or stale leader_epoch.

  Background:
    Given a 3-node cluster [N1, N2, N3] with cluster_id "cluster-abc-123"
    And N1 is Leader for term 3 (leader_epoch = 3)

  Scenario: Cluster ID mismatch rejects Fetch RPC
    When a node from a different cluster sends a Fetch RPC to N1
    And the RpcEnvelope carries cluster_id "cluster-xyz-999"
    Then N1 rejects the Fetch due to cluster_id mismatch (per architecture §6.2)
    And the rejection prevents cross-cluster contamination
    And N1 does not update any replication state for the sender

  Scenario: Cluster ID mismatch rejects Vote RPC
    When a node from a different cluster sends a Vote RPC to N2
    And the RpcEnvelope carries cluster_id "cluster-xyz-999"
    Then N2 rejects the Vote due to cluster_id mismatch
    And N2 does not update its currentTerm or votedFor
    And no election is disrupted

  Scenario: Stale leader_epoch fences messages from deposed leader
    Given N1 was Leader for term 3 (leader_epoch = 3)
    And N2 has been elected Leader for term 4 (leader_epoch = 4)
    And N3 has received a Fetch response from N2 with leader_epoch = 4
    When N1 (deposed, unaware of term 4) sends a Fetch response to N3
    And the RpcEnvelope carries leader_epoch = 3
    Then N3 rejects the message because leader_epoch 3 is stale (< N3's known epoch 4)
    And N3 does not apply any entries from the stale response
    And this prevents a deposed leader from corrupting follower state

  Scenario: Newer leader_epoch supersedes older epoch on receiver
    Given N3 has been communicating with N1 (leader_epoch = 3)
    When N2 wins election for term 4 and sends a Fetch response to N3
    And the RpcEnvelope carries leader_epoch = 4
    Then N3 accepts the message (leader_epoch 4 > N3's known epoch 3)
    And N3 updates its known leader_epoch to 4
    And subsequent messages from N1 with leader_epoch = 3 are rejected

  Scenario: Valid cluster_id and leader_epoch accepted normally
    When N2 sends a Fetch RPC to N1 with matching cluster_id "cluster-abc-123"
    And the RpcEnvelope carries leader_epoch = 3 (current epoch)
    Then N1 accepts and processes the Fetch RPC normally
    And no fencing rejection occurs

  Scenario: FetchSnapshot with cluster_id mismatch rejected
    Given N4 is a new node from a different cluster (cluster_id "cluster-other")
    When N4 sends a FetchSnapshot RPC to N1 with cluster_id "cluster-other"
    Then N1 rejects the FetchSnapshot due to cluster_id mismatch
    And no snapshot data is transferred to N4
```

---

## Feature: Client Interaction

```gherkin
Feature: Client Interaction
  As a client application embedding the xraft library
  I need to propose commands and read protocol metadata
  So that I can build a replicated state machine on top of xraft

  # Read API semantics — aligned across all four documents:
  #
  #   - read() → Result<ConsensusState> — returns a local, non-linearizable
  #     snapshot of the node's protocol metadata (term, role, leader_id, HW,
  #     voter set). Callable on any node (leader, follower, candidate, or
  #     unattached). Does NOT read application state.
  #   - Applications build their own queryable read-side state from committed
  #     records delivered via Listener::handle_commit (architecture §4.1).
  #   - Consensus metadata is available via read(); observability counters
  #     (latencies, rates) are available via RaftNode::metrics() (§6.4).
  #   - StateMachine trait has only apply, snapshot, restore — no query() method.
  #   - Commit notification uses three phases (architecture §4.1):
  #     (1) StateMachine::apply, (2) Listener::handle_commit,
  #     (3) DeferredCompletionQueue::complete. No DeferredReadQueue exists.
  #   - Tech spec §2.2 correctly marks "linearisable reads" (read-index,
  #     lease-based) as out of scope. read() makes no linearizability claims.

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
    Then the event loop invokes the three-phase commit sequence in order (architecture §4.1):
      | Phase | Action                                                                          |
      | 1     | StateMachine::apply("set x=1") — one call per committed command                 |
      | 2     | Listener::handle_commit(batch) — one batch of committed AppRecords              |
      | 3     | DeferredCompletionQueue::complete — resolves the propose future                 |
    And the propose future resolves with Ok (after phase 3)
    And all callbacks are synchronous in-process calls within the event loop

  Scenario: Proposal rejected on follower
    When a client calls propose("set x=1") on N2 (a follower)
    Then N2 returns an error indicating it is not the leader
    And the error includes the current leader's identity (N1)

  Scenario: Proposal rejected on candidate during election
    Given no leader exists and N1 is a Candidate
    When a client calls propose("set x=1") on N1
    Then N1 returns an error indicating no leader is available

  Scenario: Read protocol metadata on leader
    # Per architecture §5.11: read() → Result<ConsensusState> returns a local,
    # non-linearizable snapshot of protocol metadata. Callable on any node.
    # Does NOT read application state — applications build their own read-side
    # state from Listener::handle_commit callbacks (architecture §4.1).
    Given N1 is Leader for term 1
    When a client calls read() on N1
    Then read() returns Ok(ConsensusState) immediately (no I/O, no log append)
    And the returned ConsensusState contains:
      | Field          | Value                 |
      | current_term   | 1                     |
      | role           | Leader                |
      | leader_id      | Some(N1)              |
      | high_watermark | current HW value      |
      | voter_set      | [N1, N2, N3]          |
    And the read completes synchronously — it does not enter the event loop's message queue

  Scenario: Read on fresh leader returns current metadata (no deferral)
    # Per architecture §5.11: read() returns ConsensusState immediately
    # regardless of whether a current-term entry has been committed.
    # The returned metadata may show a stale HW if the LeaderChangeMessage
    # has not yet committed — this is expected (non-linearizable).
    Given N1 just won election for term 2 and appended a LeaderChangeMessage
    And the LeaderChangeMessage has NOT yet been committed (followers have not fetched it)
    When a client calls read() on N1
    Then read() returns Ok(ConsensusState) immediately
    And the returned ConsensusState shows role=Leader, current_term=2
    And the high_watermark reflects the pre-election committed state (not yet advanced)

  Scenario: Read on follower returns local metadata (not an error)
    # Per architecture §5.11: read() is callable on any node. On a follower,
    # it returns the follower's local ConsensusState, which may be stale
    # relative to the leader's authoritative state. No NotLeader error.
    Given N1 is Leader for term 1
    And N2 is a Follower
    When a client calls read() on N2
    Then read() returns Ok(ConsensusState)
    And the returned ConsensusState shows role=Follower, leader_id=Some(N1)
    And the high_watermark may lag behind the leader's HW
    And the client can use leader_id to direct proposals to N1

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

  Scenario: Read after leader step-down returns updated metadata
    # Per architecture §5.11: read() always returns local ConsensusState.
    # After the leader steps down (via higher-term RPC or check-quorum),
    # read() returns the updated role and leader_id — no DeferredReadQueue,
    # no error. There are no pending read futures to resolve because read()
    # is synchronous and returns immediately.
    Given N1 is Leader for term 2
    When N1 receives a Vote request from N3 at term 3 (higher term)
    Then N1 steps down to Follower (observing higher term)
    When a client calls read() on N1
    Then read() returns Ok(ConsensusState) with role=Follower, current_term=3
    And leader_id reflects the new leader if known, or None

  Scenario: Read on partitioned leader after check-quorum step-down
    # A partitioned leader steps down via check-quorum (§5.7). After
    # step-down, read() returns ConsensusState reflecting the new role.
    # Applications that built read-side state from Listener::handle_commit
    # will stop receiving new commits, and their state becomes stale —
    # this is the application's responsibility to handle.
    Given N1 is Leader for term 1
    When N1 becomes partitioned from N2 and N3
    Then N1's check-quorum deadline fires (within one election timeout interval)
    And N1 checks voter liveness: only {self} has recent Fetch (1 < majority of 2)
    And N1 steps down to Follower (check-quorum failure per §5.7)
    When a client calls read() on N1 after step-down
    Then read() returns Ok(ConsensusState) with role=Follower, leader_id=None
    And N1's metadata reflects the stale HW (no new commits while partitioned)
    When N2 wins election for term 2 and becomes the new Leader
    And a client calls read() on N2
    Then read() returns Ok(ConsensusState) with role=Leader, current_term=2

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
    And the application implements StateMachine with apply(), snapshot(), and restore()
    When entries at offsets 5–7 are committed (HW advances to 8; offsets < 8 committed)
    Then the event loop executes the three-phase commit sequence per the architecture doc (§4.1):
      | Phase | Action                                                                         |
      | 1     | StateMachine::apply — one call per committed command entry (offsets 5, 6, 7)    |
      | 2     | Listener::handle_commit — one batch of committed AppRecords [5, 6, 7]          |
      | 3     | DeferredCompletionQueue::complete — resolves client propose futures (offset<HW) |
    And control records (if any among 5–7) are handled internally and never reach StateMachine::apply
    And all three phases are synchronous in-process calls within the event loop
    And the Fetch response (via IoStage) reflects the same HW = 8 that the callbacks observed

  Scenario: Leader step-down with pending proposals — futures resolve with error
    Given N1 is Leader for term 1
    And a client has proposed "set x=1" to N1 — the entry is appended but HW has not advanced past it
    And the propose future is parked in the DeferredCompletionQueue
    When N1 receives a Vote RPC with term 3 (higher term)
    Then N1 steps down to Follower state
    And N1 drains the DeferredCompletionQueue, resolving all pending propose futures with Err(NotLeader)
    And the client can retry the proposal against the new leader

  Scenario: Proposal rejected with ProposalQueueFull when batch accumulator is at capacity
    Given N1 is Leader for term 1
    And N1's BatchAccumulator has reached its configured maximum capacity
    When a client calls propose("set x=1") on N1
    Then N1 returns Err(ProposalQueueFull) immediately without appending to the log
    And the client can retry after a backoff
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

  # RaftMetrics struct fields (per architecture doc §6.4):
  #   current_leader: Option<NodeId>    — exposes the tech spec "current-leader" metric
  #   current_epoch: u64                — exposes "current-epoch"
  #   election_latency_avg_ms: f64      — exposes "election-latency-avg"
  #   append_records_rate: f64           — exposes "append-records-rate"
  #   commit_latency_avg_ms: f64         — exposes "commit-latency-avg"
  #   high_watermark, log_end_offset, log_start_offset, role, voter_count, observer_count

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 2

  Scenario: Current leader metric is accurate on leader
    When querying RaftMetrics on N1 (the leader)
    Then RaftMetrics.current_leader returns Some(N1's node ID)
    And RaftMetrics.current_epoch returns 2

  Scenario: Current leader metric on follower reflects last known leader
    Given N2 has recently received a Fetch response from N1
    When querying RaftMetrics on N2
    Then RaftMetrics.current_leader returns Some(N1's node ID)
    And RaftMetrics.current_epoch returns 2
    # Note: a follower learns the leader identity from Fetch responses.
    # A follower that has not yet received a Fetch response may return None.

  Scenario: Leader metric updates after election
    Given N1 crashes
    And N2 wins election for term 3
    When querying RaftMetrics on N2
    Then RaftMetrics.current_leader returns Some(N2's node ID)
    And RaftMetrics.current_epoch returns 3
    When N3 receives a Fetch response from N2 (the new leader)
    And querying RaftMetrics on N3
    Then RaftMetrics.current_leader returns Some(N2's node ID)
    And RaftMetrics.current_epoch returns 3

  Scenario: Unknown leader metric during election
    Given no leader exists (election in progress)
    When querying RaftMetrics on any node
    Then RaftMetrics.current_leader returns None (per architecture doc §6.4: Option<NodeId>)

  Scenario: Election latency metric is recorded
    Given a leader election completes in 45ms
    When querying RaftMetrics on the new leader
    Then "election_latency_avg_ms" reflects the 45ms election duration

  Scenario: Append rate metric tracks leader throughput
    Given the leader N1 appends 500 entries over 10 seconds
    When querying RaftMetrics on N1
    Then "append_records_rate" is approximately 50 records/second

  Scenario: Commit latency metric tracks end-to-end commit time
    Given a client proposes a command
    And the command is committed after 12ms (propose to HW advancement)
    When querying RaftMetrics on the leader
    Then "commit_latency_avg_ms" reflects the 12ms latency
```

---

## Feature: Graceful Shutdown and Lifecycle

```gherkin
Feature: Graceful Shutdown and Lifecycle
  As a cluster operator or application embedding xraft
  I need nodes to shut down gracefully and transition through
  well-defined lifecycle states
  So that resources are cleaned up and the application is notified
  before the node stops

  # Per architecture doc §4.1 (Listener::begin_shutdown) and §4.4
  # (ReceiverTask shutdown).

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1
    And each node has a registered Listener with begin_shutdown

  Scenario: Graceful shutdown of a follower
    When the application calls shutdown() on N2
    Then N2 invokes Listener::begin_shutdown() on the application's Listener
    And N2 stops the ReceiverTask (ceases receiving inbound RPCs)
    And N2 completes any pending IoActions in the IoStage
    And N2 stops the EventLoop
    And the remaining cluster [N1, N3] continues with N1 as leader
    And N1 detects N2 as unresponsive (no Fetch RPCs) but maintains quorum with N1 + N3

  Scenario: Graceful shutdown of the leader
    When the application calls shutdown() on N1 (the leader)
    Then N1 invokes Listener::begin_shutdown()
    And N1 stops accepting new proposals (propose() returns Err(Shutdown))
    And N1 drains the DeferredCompletionQueue, resolving pending futures with Err(Shutdown)
    And N1 stops the ReceiverTask and EventLoop
    And N2 and N3 detect the leader's absence (Fetch responses stop arriving)
    And a new election occurs — one of N2 or N3 becomes the new leader

  Scenario: Shutdown signal interrupts snapshot transfer in progress
    Given N2 is in the middle of downloading a snapshot from N1 via FetchSnapshot
    When the application calls shutdown() on N2
    Then N2 aborts the in-progress snapshot transfer
    And N2 invokes Listener::begin_shutdown()
    And N2 shuts down without completing the snapshot install
    And on restart, N2 will re-initiate the snapshot transfer from the current leader

  Scenario: Unattached node lifecycle — no election, no Fetch, no proposals
    Given N4 has been removed from the voter set via RemoveVoter
    And N4 has transitioned to Unattached state after learning of its removal
    Then N4 does not start election timers
    And N4 does not send Vote RPCs
    And N4 does not send Fetch RPCs
    And propose() on N4 returns Err(NotLeader) — N4 is not part of the cluster
    And read() on N4 returns ConsensusState with role=Unattached and leader_id=None
    And N4 remains in Unattached state until it is shut down or reconfigured
```

---

## Feature: Error Recovery and Fault Tolerance

```gherkin
Feature: Error Recovery and Fault Tolerance
  As a Raft node
  I need to handle errors in storage, state machine, and network
  gracefully
  So that correctness is preserved even when components fail

  # Per architecture doc §6.3 error handling strategy. All error
  # semantics are crash-stop except where explicitly noted.

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1

  Scenario: StateMachine::apply error triggers crash-stop
    Given an entry at offset 5 has been committed (HW > 5)
    When the event loop on N2 calls StateMachine::apply(5, record) during commit processing
    And apply() returns Err (e.g., corrupt internal state)
    Then N2's event loop treats the error as irrecoverable
    And N2 invokes Listener::begin_shutdown()
    And N2 halts (crash-stop)
    And N2 does NOT skip the entry — committed entries cannot be skipped
    And the cluster continues with N1 and N3
    And on restart, N2 will re-apply the entry (the error must be fixed externally)

  Scenario: StateMachine::snapshot error is non-fatal — retry at next interval
    Given N1 has committed entries up to offset 100
    When the snapshot threshold is reached and the event loop calls StateMachine::snapshot()
    And snapshot() returns Err (e.g., transient serialization failure)
    Then N1 logs the error but does NOT halt
    And N1 continues operating normally (no crash-stop)
    And log compaction is deferred (no prefix truncation occurs)
    And at the next snapshot interval, StateMachine::snapshot() is retried
    And if it succeeds, normal compaction resumes

  Scenario: StateMachine::restore error triggers crash-stop
    Given N2 has received a snapshot via FetchSnapshot
    When the event loop on N2 calls StateMachine::restore(app_snapshot)
    And restore() returns Err (e.g., incompatible snapshot format)
    Then N2's event loop treats the error as irrecoverable
    And N2 invokes Listener::begin_shutdown()
    And N2 halts (crash-stop)
    And the cluster continues with N1 and N3

  Scenario: Listener panic aborts the event loop task
    Given the application has registered a Listener whose handle_commit panics
    When entries are committed and the event loop invokes Listener::handle_commit(batch)
    And the Listener implementation panics
    Then the panic propagates through the event loop task
    And the event loop task aborts (crash-stop)
    And N2 halts — applications must not panic in Listener callbacks

  Scenario: Malformed RPC message is dropped silently
    Given N1 is Leader and processing incoming RPCs
    When N1 receives a message that fails deserialization (corrupt or unknown payload)
    Then N1 drops the malformed message with a warning log
    And N1 does NOT update any protocol state (term, votedFor, HW, etc.)
    And N1 continues processing subsequent valid messages normally

  Scenario: Network send failure is tolerated in pull-based model
    Given N1 is Leader and processing a Fetch request from N2
    When the IoStage calls TransportSender::send() to deliver the Fetch response to N2
    And send() fails (e.g., connection reset)
    Then N1 logs the error but does NOT halt
    And N1 continues operating normally
    And N2 will retry its Fetch RPC on the next fetch interval
    And the pull-based model makes missed responses equivalent to slow followers

  Scenario: Storage I/O failure during log append triggers crash-stop
    Given a client proposes a command to N1
    And N1 stages the entry in the BatchAccumulator
    When the IoStage calls LogStore::append() and it returns Err (disk failure)
    Then N1's event loop treats the error as irrecoverable
    And N1 invokes Listener::begin_shutdown()
    And N1 halts (crash-stop — operating with potentially corrupt state is unsafe)
    And the cluster elects a new leader from [N2, N3]

  Scenario: QuorumStateStore save failure triggers crash-stop
    Given N2 receives a Vote RPC and needs to persist its vote
    When QuorumStateStore::save() returns Err (disk failure)
    Then N2 does NOT acknowledge the vote (unsafe without durable persistence)
    And N2 invokes Listener::begin_shutdown()
    And N2 halts (crash-stop)
```

---

## Feature: Log Integrity and CRC Recovery

```gherkin
Feature: Log Integrity and CRC Recovery
  As a Raft node recovering from a crash
  I need to validate log segment integrity using CRC-32C checksums
  So that corrupt or partially written entries are detected and safely
  truncated without losing committed data

  # Per tech spec §6 (Key Design Decisions — Log integrity: CRC-32C per batch)
  # and risk R7 (Log/snapshot corruption and torn writes).

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And log segments use CRC-32C checksums per batch

  Scenario: Clean recovery with all CRCs valid
    Given N1 has committed entries at offsets 0–50 and persisted them with valid CRCs
    When N1 crashes and restarts
    Then N1 scans all log segments from the start
    And every batch CRC validates successfully
    And N1's log is intact with all 51 entries (offsets 0–50)
    And N1 resumes as Follower with the full log

  Scenario: Recovery with corrupt CRC truncates at first bad batch
    Given N1 has committed entries at offsets 0–50 persisted across two batches:
      | Batch | Offsets | CRC    |
      | 1     | 0–30    | valid  |
      | 2     | 31–50   | corrupt (bit-flip in stored CRC or data) |
    When N1 crashes and restarts
    Then N1 scans log segments forward from the start
    And N1 validates batch 1 (offsets 0–30) — CRC OK
    And N1 encounters batch 2 — CRC mismatch detected
    And N1 truncates batch 2 and everything after it
    And N1's recovered log contains offsets 0–30 only (log_end_offset = 31)
    And entries at offsets 31–50 are lost (they were uncommitted tail entries)
    And N1 resumes as Follower and Fetches missing entries from the leader

  Scenario: Torn write recovery — partial batch at end of segment
    # A crash during fsync can leave an incomplete batch at the end of a
    # segment file. The partial batch has no valid CRC trailer.
    Given N1 was appending a batch covering offsets 45–50 when it crashed
    And the batch write was interrupted — only a partial record was written to disk
    When N1 restarts and scans the log
    Then N1 reads complete batches for offsets 0–44 successfully
    And N1 encounters the partial batch at the end of the segment
    And N1 truncates the partial batch (it has no valid CRC)
    And N1's recovered log contains offsets 0–44 (log_end_offset = 45)
    And earlier committed entries (offsets 0–44 if HW was ≥ 45) remain intact
    And N1 resumes and Fetches entries from offset 45 from the leader

  Scenario: Crash during snapshot write leaves temp file ignored
    # Per architecture §2.2 (SnapshotStore): snapshot writes are atomic
    # (write-to-temp, fsync, rename). A crash during the write leaves
    # only a temporary file that is never renamed.
    Given N1 is writing a snapshot at offset 100 to a temp file
    When N1 crashes during the snapshot fsync (before the rename to final path)
    Then on restart, the temp file is present but the final snapshot file at offset 100 does not exist
    And N1 ignores the incomplete temp file (it was never atomically renamed)
    And N1 falls back to the previous snapshot (if any) or starts with no snapshot
    And N1 recovers using the log entries available on disk
    And the incomplete temp file is cleaned up

  Scenario: Corrupt latest snapshot on startup — fall back to prior snapshot
    # If the latest snapshot file is corrupt (e.g., bad checksum in the
    # snapshot header or payload), the node should fall back to the
    # previous good snapshot if one exists, or fail closed if none.
    Given N1 has two snapshots on disk:
      | Snapshot | last_included_offset | Status  |
      | S1       | 50                   | valid   |
      | S2       | 100                  | corrupt |
    When N1 restarts and attempts to load the latest snapshot (S2)
    Then N1 detects S2 is corrupt (checksum mismatch)
    And N1 falls back to the previous snapshot S1 (last_included_offset = 50)
    And N1 restores StateMachine from S1's AppSnapshot
    And N1 sets HW = 51 (snapshot.last_included_offset + 1)
    And N1 replays log entries from offset 51 onward
    And N1 logs a warning about the corrupt snapshot S2

  Scenario: Corrupt only snapshot on startup — crash-stop
    Given N1 has one snapshot on disk at offset 100 and it is corrupt
    And N1's log has been prefix-truncated (log_start_offset > 0, entries before snapshot are gone)
    When N1 restarts and attempts to load the snapshot
    Then N1 detects the snapshot is corrupt
    And N1 has no fallback snapshot
    And N1 cannot reconstruct state (log entries before LSO are compacted away)
    And N1 fails to start with an irrecoverable error
    And the error must be resolved externally (restore from backup or re-provision)
```

---

## Feature: Single-Node Cluster

```gherkin
Feature: Single-Node Cluster
  As a developer or cluster operator
  I need a single-node cluster to function correctly
  So that I can use combined-mode for development and testing,
  and so that single-node deployments are a valid configuration

  # Per Confluent article: combined-mode for single-node testing.
  # A single-node cluster is the simplest valid Raft configuration.

  Scenario: Single-node cluster self-elects immediately
    Given a 1-node cluster [N1] bootstrapped with voter set [N1]
    When N1's election timeout expires
    Then N1 transitions to Candidate for term 1
    And N1 votes for itself (1 of 1 — immediate majority)
    And N1 transitions to Leader for term 1
    And N1 appends a LeaderChangeMessage at offset 0, term 1
    And N1 is simultaneously the only voter and the leader

  Scenario: Single-node cluster commits entries without followers
    Given a 1-node cluster [N1] where N1 is Leader for term 1
    And N1 has appended a LeaderChangeMessage at offset 0
    When a client proposes command "set x=1" to N1
    Then N1 appends the entry at offset 1, term 1 (log_end_offset = 2)
    And N1 recalculates HW: only 1 voter with log_end_offset = 2
    And sorted desc [2] → index ⌊1/2⌋ = 0 → HW candidate = 2
    And HW advances to 2 (entries 0 and 1 committed)
    And the entry is committed immediately without any Fetch round
    And StateMachine::apply is called for the command entry
    And the propose future resolves with Ok

  Scenario: Single-node check-quorum always passes
    Given a 1-node cluster [N1] where N1 is Leader
    When the check-quorum interval elapses
    Then N1 counts itself as reachable (1 of 1 — majority)
    And N1 remains Leader
    And check-quorum never causes step-down in a single-node cluster

  Scenario: Single-node cluster grows to 2 then 3 via sequential AddVoter
    # Per architecture §5.5: only one voter change at a time.
    # Growing from 1 to 3 requires two sequential AddVoter operations.
    Given a 1-node cluster [N1] where N1 is Leader with voter set [N1]
    And N2 has joined as an Observer and caught up (fetch_offset ≥ HW)
    When a client sends AddVoter for N2 to N1
    Then N1 appends a VotersRecord [N1, N2]
    And HW advancement uses the new voter set [N1, N2] (majority = 2 of 2)
    When N2 replicates and confirms the VotersRecord (both N1 and N2 have it)
    Then the VotersRecord commits and the active voter set is [N1, N2]
    And the quorum size is now 2 (majority of 2)
    # Second change: 2 → 3
    Given N3 has joined as an Observer and caught up
    When a client sends AddVoter for N3 to N1
    Then N1 appends a VotersRecord [N1, N2, N3]
    And HW advancement uses [N1, N2, N3] (majority = 2 of 3)
    When a majority of [N1, N2, N3] replicates the VotersRecord
    Then the VotersRecord commits and the active voter set is [N1, N2, N3]
    And the cluster is now a standard 3-node cluster

  Scenario: Single-node snapshot and recovery
    Given a 1-node cluster [N1] with committed entries at offsets 0–100
    When N1 takes a snapshot at offset 100 and performs prefix truncation
    And N1 crashes and restarts
    Then N1 loads the snapshot (last_included_offset = 100)
    And N1 restores StateMachine from the snapshot
    And N1 sets HW = 101
    And N1 resumes as Follower, then self-elects as Leader for term 2
    And normal operation continues
```

---

## Feature: Batch Accumulation and Group Commit

```gherkin
Feature: Batch Accumulation and Group Commit
  As a Raft leader
  I need to batch multiple proposals into a single log append and fsync
  So that throughput is improved by amortising fsync cost across
  multiple client proposals

  # Per architecture §2.1 (BatchAccumulator): proposals are staged and
  # drained on each event-loop tick or when the batch is full. This
  # amortises fsync cost (group commit). Adapted from KRaft's
  # BatchAccumulator.

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1
    And the BatchAccumulator max_batch_size is configured to 10

  Scenario: Multiple proposals batched into single log append
    When 3 clients concurrently propose commands "set x=1", "set y=2", "set z=3" to N1
    Then all 3 entries are staged in the BatchAccumulator
    When the event-loop tick fires (drain interval)
    Then the BatchAccumulator drains all 3 entries into a single IoAction::AppendLog
    And LogStore::append is called once with 3 entries (not 3 separate calls)
    And a single fsync covers all 3 entries (group commit)
    And each propose future is parked in the DeferredCompletionQueue at its respective offset

  Scenario: Batch drains when max_batch_size is reached
    When 10 clients propose commands to N1 in rapid succession
    Then the BatchAccumulator reaches its configured capacity of 10
    And the batch is drained immediately (before the next tick)
    And all 10 entries are appended to the log in a single IoAction::AppendLog

  Scenario: Batch drains on tick even with partial fill
    When 2 clients propose commands to N1
    And the batch is not full (2 < max_batch_size of 10)
    When the event-loop tick fires
    Then the BatchAccumulator drains the 2 pending entries
    And they are appended to the log in a single IoAction::AppendLog
    And fsync cost is amortised even for a small batch

  Scenario: Empty batch — tick with no pending proposals
    Given no new proposals have been submitted since the last drain
    When the event-loop tick fires
    Then the BatchAccumulator has no entries to drain
    And no IoAction::AppendLog is emitted
    And no unnecessary fsync occurs

  Scenario: Proposals submitted after drain are staged in next batch
    When a client proposes "set a=1" to N1
    And the event-loop tick fires (draining "set a=1")
    And then another client proposes "set b=2" before the next tick
    Then "set b=2" is staged in a new batch
    And it will be drained on the next tick or when the batch fills
    And "set a=1" and "set b=2" are at consecutive log offsets

  Scenario: Group commit — all propose futures in batch resolve together on HW advance
    Given 3 proposals were batched and appended at offsets 5, 6, 7
    And all 3 propose futures are parked in the DeferredCompletionQueue
    When followers replicate and HW advances to 8 (offsets < 8 committed)
    Then the DeferredCompletionQueue fires all 3 futures (offsets 5, 6, 7 all < 8)
    And all 3 clients receive Ok simultaneously
```

---

## Feature: Stale and Delayed Message Handling

```gherkin
Feature: Stale and Delayed Message Handling
  As a Raft node
  I need to correctly handle stale, delayed, duplicated, and
  reordered messages
  So that the protocol remains correct even under adverse network
  conditions without requiring reliable ordered delivery

  # These scenarios test protocol resilience to network non-idealities
  # that the deterministic simulation harness can inject. Per tech spec
  # §4.2: messages may be delayed, reordered, duplicated, or lost.

  Background:
    Given a 3-node cluster [N1, N2, N3]

  Scenario: Candidate receives late granted vote after stepping down — ignored
    Given N1 was a Candidate for term 5 but stepped down to Follower
    (received a higher-term message, now at term 6)
    When N1 receives a late VoteResponse { term: 5, vote_granted: true }
    Then N1 ignores the response because it is no longer a Candidate
    And N1's state is unchanged (remains Follower at term 6)
    And the stale vote does not trigger a leadership transition

  Scenario: Follower receives delayed Fetch response from old leader — fenced
    Given N1 was Leader for term 3 and sent Fetch responses to N2
    And N2 has since received a Fetch response from N3 (new leader, term 4)
    And N2's known leader_epoch is now 4
    When N2 receives a delayed Fetch response from N1 with leader_epoch = 3
    Then N2 rejects the response because leader_epoch 3 < known epoch 4
    And N2 does NOT apply entries from the stale response
    And N2 does NOT reset its election timeout from the stale response

  Scenario: Duplicate Fetch RPC from follower is idempotent
    Given N1 is Leader for term 1
    And N2 sends a Fetch RPC with fetch_offset = 5
    When N1 receives the same Fetch RPC again (network duplicate)
    Then N1 processes it identically to the first
    And N1's recorded fetch_offset for N2 remains 5 (not double-incremented)
    And the Fetch response is identical
    And HW calculation is not affected by the duplicate

  Scenario: Duplicate Vote RPC is idempotent
    Given N1 has already voted for N2 in term 5
    When N1 receives the same Vote RPC from N2 for term 5 again (duplicate)
    Then N1 responds with vote_granted = true (same vote, idempotent)
    And N1's votedFor remains N2
    And no additional quorum-state persistence is required

  Scenario: Reordered Fetch responses — older HW does not regress local HW
    Given N2 is a Follower with local HW = 10
    When N2 receives a Fetch response with HW = 8 (delayed older response)
    Then N2 does NOT decrease its local HW from 10 to 8
    And N2's HW remains 10 (HW never decreases)
    When N2 later receives a Fetch response with HW = 15
    Then N2 advances its local HW to 15

  Scenario: Asymmetric network partition — leader to follower works, follower to leader fails
    # In an asymmetric partition, the leader can receive Fetch RPCs from
    # some nodes but those nodes' responses cannot reach others.
    Given N1 is Leader for term 1
    And the network is asymmetric: N2→N1 works (Fetch RPCs delivered) but N1→N2 fails (responses dropped)
    When N2 sends Fetch RPCs to N1
    Then N1 receives the Fetch and records N2's fetch_offset (HW may advance)
    But N2 never receives the Fetch response (responses are dropped)
    And N2's election timeout eventually fires (no Fetch response = no heartbeat)
    And N2 initiates an election
    And the cluster resolves the asymmetric partition via leader election

  Scenario: Message from unknown node is rejected
    Given the voter set is [N1, N2, N3]
    When N1 receives a Fetch RPC from N99 (an unknown node not in voter or observer sets)
    Then N1 does not track replication state for N99
    And N1 does not count N99 toward HW advancement
    And N1 rejects or ignores the Fetch (N99 is not a cluster member)
```

---

## Feature: Sequential Membership Changes

```gherkin
Feature: Sequential Membership Changes
  As a cluster operator
  I need to perform multiple membership changes sequentially
  So that the cluster can be scaled up, scaled down, or have
  nodes replaced over time without violating the single-change invariant

  # Per architecture §5.5: only one voter change at a time. Multiple
  # changes must be serialised — each VotersRecord must commit before
  # the next change is submitted.

  Background:
    Given a 3-node cluster [N1, N2, N3]
    And N1 is Leader for term 1

  Scenario: Add N4 then add N5 sequentially
    Given N4 is an Observer caught up with the leader
    When a client sends AddVoter for N4 to N1
    Then N1 appends VotersRecord [N1, N2, N3, N4]
    When the VotersRecord commits (majority of [N1, N2, N3, N4] = 3 of 4)
    Then the active voter set is [N1, N2, N3, N4]
    # Second change — only possible after first commits
    Given N5 is an Observer caught up with the leader
    When a client sends AddVoter for N5 to N1
    Then N1 appends VotersRecord [N1, N2, N3, N4, N5]
    When the VotersRecord commits (majority of [N1, N2, N3, N4, N5] = 3 of 5)
    Then the active voter set is [N1, N2, N3, N4, N5]
    And both changes completed without violating the single-change invariant

  Scenario: Remove N3 then remove N2 — cluster shrinks from 3 to 1
    When a client sends RemoveVoter for N3 to N1
    Then N1 appends VotersRecord [N1, N2]
    When the VotersRecord commits (majority of [N1, N2] = 2 of 2)
    Then the active voter set is [N1, N2] and N3 transitions to Unattached
    # Second change
    When a client sends RemoveVoter for N2 to N1
    Then N1 appends VotersRecord [N1]
    When the VotersRecord commits (majority of [N1] = 1 of 1)
    Then the active voter set is [N1] and N2 transitions to Unattached
    And the cluster is now a single-node cluster with N1 as the sole voter

  Scenario: Replace a node — remove N3, add N4 sequentially
    When a client sends RemoveVoter for N3 to N1
    Then N1 appends VotersRecord [N1, N2]
    When the VotersRecord commits
    Then the active voter set is [N1, N2]
    # Now add the replacement
    Given N4 is an Observer caught up with the leader
    When a client sends AddVoter for N4 to N1
    Then N1 appends VotersRecord [N1, N2, N4]
    When the VotersRecord commits (majority of [N1, N2, N4] = 2 of 3)
    Then the active voter set is [N1, N2, N4]
    And N3 is no longer a member; N4 has replaced it

  Scenario: Sequential changes interleaved with leader election
    Given the voter set is [N1, N2, N3]
    And N4 is an Observer caught up with the leader
    When a client sends AddVoter for N4 to N1
    And N1 appends VotersRecord [N1, N2, N3, N4]
    And the VotersRecord commits — voter set becomes [N1, N2, N3, N4]
    And then N1 crashes
    When N2 wins election for term 2 and becomes Leader
    Then N2 uses voter set [N1, N2, N3, N4] for election quorum (4 voters, need 3)
    Given N5 is an Observer caught up with the new leader N2
    When a client sends AddVoter for N5 to N2
    Then N2 appends VotersRecord [N1, N2, N3, N4, N5]
    And the change proceeds under N2's leadership
    And the single-change invariant is maintained across the leader transition

  Scenario: Second change rejected while first is in flight
    Given a client sends AddVoter for N4 to N1
    And N1 appends VotersRecord [N1, N2, N3, N4] but it is NOT yet committed
    When a client sends RemoveVoter for N3 to N1
    Then N1 rejects the RemoveVoter with MembershipError::ChangeInProgress
    And the client must wait for the first change to commit before retrying
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
| §2.1.1 Persistence | Persistence and Crash Recovery | 9 |
| §2.1.2 Snapshotting | Log Compaction and Snapshots | 5 |
| §2.1.2 Snapshot transfer | Snapshot Transfer | 5 |
| §2.1.2 Log truncation / Divergence | Log Divergence and Truncation | 4 |
| §2.1.3 Single-node changes | Add Voter / Remove Voter / UpdateVoter | 15 |
| §2.1.3 Non-voting members (observers) | Observer Promotion | 4 |
| §2.1.3 Leader step-down | Remove Voter (scenario 2) | — |
| §2.1.3 Sequential membership changes | Sequential Membership Changes | 5 |
| §2.1.4 Identity & fencing | Identity and Fencing | 6 |
| §2.1.4 Divergence detection | Log Divergence and Truncation | — |
| §2.1.5 Library API | Client Interaction | 14 |
| §2.1.5 Batch accumulator | Batch Accumulation and Group Commit | 6 |
| §2.1.6 Metrics | Observability and Metrics | 7 |
| §2.1.6 Deterministic simulation | Safety Invariants (scenario 6) | — |
| §2.1.7 Bootstrap & recovery | Cluster Bootstrap | 5 |
| §4.1 Listener lifecycle | Graceful Shutdown and Lifecycle | 4 |
| §4.2 Failure model (message loss/reorder) | Stale and Delayed Message Handling | 7 |
| §6 Log integrity (CRC-32C) | Log Integrity and CRC Recovery | 6 |
| §6.3 Error handling strategy | Error Recovery and Fault Tolerance | 8 |
| Combined-mode / single node | Single-Node Cluster | 5 |
| **Total** | **24 Features** | **152 Scenarios** |
