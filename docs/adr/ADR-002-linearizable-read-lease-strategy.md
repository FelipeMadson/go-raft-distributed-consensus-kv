# ADR-002: Linearizable Read & Deduplication Strategy

## Status
Accepted

## Context
In a distributed key-value store, reading data directly from the leader's local memory risks returning stale data if the leader has been partitioned away and a new leader has already been elected elsewhere (the "phantom leader" problem).

To achieve strict **Linearizability** (strong consistency), read operations must observe the effects of all committed writes that preceded them in real-time order.

Two patterns were evaluated:
1. **Log-Replicated Reads**: Each read is packaged as an entry and committed through the standard Raft log replication pipeline.
2. **Leader Leases**: The leader relies on a synchronized clock lease to serve reads without log commits.

## Decision
We implemented **Log-Replicated Reads** with client request deduplication:
1. **Log Commit for Reads**: Read operations are proposed to the Raft log and committed across a quorum before returning to the caller. This ensures the reading node is still the legitimate leader recognized by a majority.
2. **Client Sequence Deduplication**: Each client provides a unique `ClientID` and monotonically increasing `SeqNum`. The state machine maintains a `lastAppliedSeq` registry to discard duplicate proposals upon network retries, guaranteeing **exactly-once semantics**.

## Consequences
- **Positive**: Strict linearizability is guaranteed without relying on synchronized wall-clock assumptions (avoiding TrueTime / NTP drift pitfalls).
- **Trade-off**: Read latency incurs one network round-trip for quorum consensus. For high read workloads, a future optimization will introduce ReadIndex / LeaseRead with heartbeats.
