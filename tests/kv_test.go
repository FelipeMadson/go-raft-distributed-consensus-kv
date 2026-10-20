package tests

import (
	"testing"
	"time"

	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/kv"
	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/raft"
	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/transport"
)

type kvCluster struct {
	net      *transport.Network
	nodes    []*raft.RaftNode
	servers  []*kv.KVServer
	applyChs []chan raft.ApplyMsg
	n        int
}

func newKVCluster(n int) *kvCluster {
	net := transport.NewNetwork()
	kc := &kvCluster{
		net:      net,
		nodes:    make([]*raft.RaftNode, n),
		servers:  make([]*kv.KVServer, n),
		applyChs: make([]chan raft.ApplyMsg, n),
		n:        n,
	}

	for i := 0; i < n; i++ {
		storage := raft.NewMemoryStorage()
		kc.applyChs[i] = make(chan raft.ApplyMsg, 100)

		var peers []int
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}

		kc.nodes[i] = raft.NewRaftNode(i, peers, storage, kc.net, kc.applyChs[i])
		kc.servers[i] = kv.NewKVServer(i, kc.nodes[i], kc.applyChs[i])
	}

	return kc
}

func (kc *kvCluster) getLeaderKV() (*kv.KVServer, int) {
	for it := 0; it < 20; it++ {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < kc.n; i++ {
			_, isLeader := kc.nodes[i].GetState()
			if isLeader {
				return kc.servers[i], i
			}
		}
	}
	return nil, -1
}

func (kc *kvCluster) cleanup() {
	for i := 0; i < kc.n; i++ {
		kc.servers[i].Close()
	}
}

func TestKVBasicOperations(t *testing.T) {
	kc := newKVCluster(3)
	defer kc.cleanup()

	srv, _ := kc.getLeaderKV()
	if srv == nil {
		t.Fatalf("failed to obtain leader KV server")
	}

	// 1. Put
	err := srv.Put("client-1", 1, "config.timeout", "500ms")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Get
	val, exists, err := srv.Get("config.timeout")
	if err != nil || !exists || val != "500ms" {
		t.Fatalf("Get mismatch: val=%s exists=%v err=%v", val, exists, err)
	}

	// 3. Put Update
	err = srv.Put("client-1", 2, "config.timeout", "1000ms")
	if err != nil {
		t.Fatalf("Put update failed: %v", err)
	}

	val, exists, err = srv.Get("config.timeout")
	if err != nil || !exists || val != "1000ms" {
		t.Fatalf("Get after update mismatch: val=%s exists=%v err=%v", val, exists, err)
	}

	// 4. Duplicate SeqNum should be ignored (idempotent linearizability)
	err = srv.Put("client-1", 2, "config.timeout", "MALICIOUS_OVERWRITE")
	if err != nil {
		t.Fatalf("Duplicate put returned error: %v", err)
	}

	val, exists, err = srv.Get("config.timeout")
	if err != nil || !exists || val != "1000ms" {
		t.Fatalf("Deduplication failed: expected 1000ms, got %s", val)
	}

	// 5. Delete
	err = srv.Delete("client-1", 3, "config.timeout")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	val, exists, err = srv.Get("config.timeout")
	if err != nil || exists {
		t.Fatalf("Key should have been deleted: val=%s exists=%v", val, exists)
	}
}
