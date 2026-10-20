package tests

import (
	"testing"
	"time"
)

func TestNetworkPartitionTolerance(t *testing.T) {
	c := newCluster(5)
	defer c.cleanup()

	leader := c.checkLeader()
	if leader < 0 {
		t.Fatalf("cluster failed to elect initial leader")
	}

	// Create partition:
	// Majority: [0, 1, 2]
	// Minority: [3, 4]
	c.net.Partition([][]int{
		{0, 1, 2},
		{3, 4},
	})

	time.Sleep(300 * time.Millisecond)

	// In majority partition, there must be a leader
	majLeader := -1
	for _, id := range []int{0, 1, 2} {
		_, isLeader := c.nodes[id].GetState()
		if isLeader {
			majLeader = id
			break
		}
	}

	if majLeader < 0 {
		// Wait a bit more for election in majority partition
		for it := 0; it < 20; it++ {
			time.Sleep(100 * time.Millisecond)
			for _, id := range []int{0, 1, 2} {
				_, isLeader := c.nodes[id].GetState()
				if isLeader {
					majLeader = id
					break
				}
			}
			if majLeader >= 0 {
				break
			}
		}
	}

	if majLeader < 0 {
		t.Fatalf("majority partition [0, 1, 2] failed to maintain or elect a leader")
	}

	// Minority partition should NOT be able to commit
	for _, minNode := range []int{3, 4} {
		_, _, isLeader := c.nodes[minNode].Start([]byte("INVALID_MINORITY_WRITE"))
		if isLeader {
			t.Logf("minority node %d claimed leadership temporarily", minNode)
		}
	}

	// Majority leader CAN commit
	cmd := []byte("VALID_MAJORITY_WRITE")
	idx, _, isLeader := c.nodes[majLeader].Start(cmd)
	if !isLeader {
		t.Fatalf("majority leader %d failed to accept write", majLeader)
	}

	// Verify majority applied
	applied := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && applied < 2 {
		applied = 0
		for _, id := range []int{0, 1, 2} {
			select {
			case msg := <-c.applyChs[id]:
				if msg.CommandValid && msg.CommandIndex == idx {
					applied++
				}
			default:
			}
		}
		if applied < 2 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	if applied < 2 {
		t.Fatalf("majority failed to commit log entry during partition")
	}

	// Heal the network partition
	c.net.Heal()
	time.Sleep(300 * time.Millisecond)

	// Verify entire cluster is healthy
	leaderAfterHeal := c.checkLeader()
	if leaderAfterHeal < 0 {
		t.Fatalf("failed to have stable leader after healing partition")
	}
}
