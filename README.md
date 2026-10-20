# Go Raft Distributed Consensus Key-Value Store

[![CI](https://github.com/FelipeMadson/go-raft-distributed-consensus-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/FelipeMadson/go-raft-distributed-consensus-kv/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8.svg)](https://golang.org)

A high-performance, fault-tolerant distributed Key-Value storage engine built from scratch in Go 1.22+ implementing the **Raft Distributed Consensus Protocol** (Ongaro & Ousterhout).

Engineered for zero-external-dependency execution, strong linearizability, automatic split-brain prevention, and resilient recovery under severe network partitions.

---

## Architectural Overview

```
                      +-----------------------------+
                      |      Client Application     |
                      +-----------------------------+
                                     |
                                (HTTP / RPC)
                                     v
                       +---------------------------+
                       |   Leader Node (Raft KV)   |
                       |  - Propose Log Entry      |
                       |  - Wait Quorum (N/2 + 1)  |
                       +---------------------------+
                        /                         \
            AppendEntries (Term T)      AppendEntries (Term T)
                      /                             \
                     v                               v
       +---------------------------+   +---------------------------+
       |   Follower Node (Peer 1)  |   |   Follower Node (Peer 2)  |
       |  - Replicate Log          |   |  - Replicate Log          |
       |  - Commit & Apply         |   |  - Commit & Apply         |
       +---------------------------+   +---------------------------+
```

### Core Raft Guarantees Implemented

1. **Election Safety**: At most one leader can be elected in a given term.
2. **Leader Append-Only**: A leader never overwrites or truncates its own entries; it only appends new entries.
3. **Log Matching Property**: If two logs contain an entry with the same index and term, the logs are identical in all entries up to the given index.
4. **Leader Completeness**: If a log entry is committed in a given term, that entry is present in the logs of the leaders for all higher-numbered terms.
5. **State Machine Safety**: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

---

## Consensus Engine Architecture

- **Randomized Election Timeouts**: Adaptive randomized election intervals (150ms–300ms) prevent split-vote livelocks.
- **Heartbeat & Log Replication Loop**: Pipelined `AppendEntries` RPC transmissions with exponential backoff on network drop.
- **Conflict Backtracking**: Fast log reconciliation using `ConflictIndex` and `ConflictTerm` during network recovery.
- **Linearizable Reads & Deduplication**: Client mutations incorporate sequential monotonicity (`ClientID` + `SeqNum`) guaranteeing idempotent execution under retries.

---

## Directory Structure

```
├── .github/workflows/
│   └── ci.yml                 # Matrix test suite with Go -race detector
├── cmd/
│   └── server/
│       └── main.go            # Multi-node in-process cluster & HTTP gateway
├── deploy/
│   └── prometheus.yml         # Prometheus metrics scrape config
├── docs/
│   └── adr/
│       ├── ADR-001-raft-consensus-protocol-selection.md
│       └── ADR-002-linearizable-read-lease-strategy.md
├── pkg/
│   ├── kv/
│   │   └── kv.go              # Linearizable KV state machine & dedup registry
│   ├── raft/
│   │   ├── raft.go            # Core Raft state machine (Follower/Candidate/Leader)
│   │   ├── rpc.go             # RequestVote & AppendEntries definitions
│   │   └── storage.go         # Persistent state engine
│   └── transport/
│       └── network.go         # In-memory RPC simulation with partition controls
├── tests/
│   ├── election_test.go       # Election safety & failover tests
│   ├── kv_test.go             # Replicated KV CRUD & deduplication tests
│   ├── partition_test.go      # Majority vs minority partition tolerance tests
│   └── replication_test.go    # Quorum log commitment tests
├── docker-compose.yml         # 3-node containerized cluster deployment
├── Dockerfile                 # Multi-stage zero-cgo Alpine build
├── go.mod                     # Go 1.22 module definition
└── LICENSE                    # MIT License
```

---

## Quickstart

### Running Tests
Execute the comprehensive test suite with Go's race detector enabled:
```bash
go test -v -race ./...
```

### Running the Cluster Locally
```bash
go run ./cmd/server
```

The gateway listens on `http://localhost:8080`.

#### Writing a Replicated Key-Value Pair:
```bash
curl -X POST http://localhost:8080/put \
  -H "Content-Type: application/json" \
  -d '{"key": "cluster.version", "value": "2026.4.1"}'
```

#### Reading with Linearizable Guarantees:
```bash
curl "http://localhost:8080/get?key=cluster.version"
```

#### Inspecting Cluster Health & Leadership:
```bash
curl http://localhost:8080/cluster
```

---

## Architectural Decision Records (ADRs)

- [ADR-001: Raft Consensus Protocol Selection](docs/adr/ADR-001-raft-consensus-protocol-selection.md)
- [ADR-002: Linearizable Read & Deduplication Strategy](docs/adr/ADR-002-linearizable-read-lease-strategy.md)

---

## Author

**Felipe Madison**  
Portfolio: [felipemadson.github.io/portfolio](https://felipemadson.github.io/portfolio/)  
GitHub: [@FelipeMadson](https://github.com/FelipeMadson)
