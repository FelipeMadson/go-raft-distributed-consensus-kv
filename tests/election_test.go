package tests

import (
	"testing"
	"time"

	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/raft"
	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/transport"
)

type cluster struct {
	net      *transport.Network
	nodes    []*raft.RaftNode
	storages []raft.Storage
	applyChs []chan raft.ApplyMsg
	n        int
}

func newCluster(n int) *cluster {
	net := transport.NewNetwork()
	c := &cluster{
		net:      net,
		nodes:    make([]*raft.RaftNode, n),
		storages: make([]raft.Storage, n),
		applyChs: make([]chan raft.ApplyMsg, n),
		n:        n,
	}

	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		c.applyChs[i] = make(chan raft.ApplyMsg, 100)

		var peers []int
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}

		c.nodes[i] = raft.NewRaftNode(i, peers, c.storages[i], c.net, c.applyChs[i])
	}

	return c
}

func (c *cluster) checkLeader() int {
	for it := 0; it < 20; it++ {
		time.Sleep(100 * time.Millisecond)

		leaders := make([]int, 0)
		for i := 0; i < c.n; i++ {
			_, isLeader := c.nodes[i].GetState()
			if isLeader {
				leaders = append(leaders, i)
			}
		}

		if len(leaders) == 1 {
			return leaders[0]
		}
	}
	return -1
}

func (c *cluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.nodes[i].Kill()
	}
}

func TestInitialElection(t *testing.T) {
	c := newCluster(3)
	defer c.cleanup()

	leader := c.checkLeader()
	if leader < 0 {
		t.Fatalf("expected one leader to be elected, got %d", leader)
	}

	term, isLeader := c.nodes[leader].GetState()
	if !isLeader || term < 1 {
		t.Fatalf("leader %d state invalid: term %d, isLeader %v", leader, term, isLeader)
	}
}

func TestReElectionAfterLeaderDisconnect(t *testing.T) {
	c := newCluster(3)
	defer c.cleanup()

	leader1 := c.checkLeader()
	if leader1 < 0 {
		t.Fatalf("expected initial leader")
	}

	// Disconnect original leader
	c.net.Disconnect(leader1)

	// Remaining 2 nodes should elect a new leader
	leader2 := -1
	for it := 0; it < 30; it++ {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < c.n; i++ {
			if i == leader1 {
				continue
			}
			_, isLeader := c.nodes[i].GetState()
			if isLeader {
				leader2 = i
				break
			}
		}
		if leader2 != -1 {
			break
		}
	}

	if leader2 < 0 || leader2 == leader1 {
		t.Fatalf("expected new leader from survivors, got %d", leader2)
	}

	// Reconnect original leader, should become follower
	c.net.Connect(leader1)
	time.Sleep(200 * time.Millisecond)

	_, isLeader := c.nodes[leader1].GetState()
	if isLeader {
		t.Fatalf("former leader %d should have stepped down", leader1)
	}
}
