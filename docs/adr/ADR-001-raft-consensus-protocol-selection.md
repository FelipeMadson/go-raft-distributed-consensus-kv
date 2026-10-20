# ADR-001: Raft Consensus Protocol Selection

## Status
Accepted

## Context
A mission-critical distributed key-value store requires high availability, strong consistency, and automatic failover in the presence of network partitions, latency spikes, and server crashes. The two primary state machine replication protocols considered were **Multi-Paxos** and **Raft**.

Multi-Paxos historically suffered from extreme implementation ambiguity, corner cases in log hole compaction, and subtle consensus bugs due to its symmetric peer design. Raft was designed specifically to decompose consensus into distinct, independently verifiable sub-problems:
1. Leader Election
2. Log Replication
3. State Safety (Election Restriction & Log Invariant)

## Decision
We chose **Raft** as the core consensus foundation for the following architectural reasons:
1. **Strong Leader Paradigm**: Only the leader accepts writes and forces follower logs to match its own. This eliminates symmetric dual-proposal conflicts.
2. **Deterministic Log Invariant**: If two entries in different logs have the same index and term, they are guaranteed to store the exact same command, and their logs are identical up to that index.
3. **Formal Verification and Intelligibility**: The state machine rules allow formal verification with model checkers (e.g., TLA+) and straightforward unit testing via simulated transport networks.

## Consequences
- **Positive**: Clear invariant enforcement; predictable leader failover within 150-300ms; deterministic recovery from split-brain scenarios.
- **Trade-off**: High write throughput requires batching and pipelined RPCs because all client writes must traverse the single active leader node.
