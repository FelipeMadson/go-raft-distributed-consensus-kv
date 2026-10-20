package tests

import (
	"bytes"
	"testing"
	"time"
)

func TestBasicLogReplication(t *testing.T) {
	c := newCluster(3)
	defer c.cleanup()

	leader := c.checkLeader()
	if leader < 0 {
		t.Fatalf("cluster failed to elect leader")
	}

	command := []byte("SET telemetry_rate 5000")
	index, _, isLeader := c.nodes[leader].Start(command)
	if !isLeader {
		t.Fatalf("expected node %d to still be leader", leader)
	}

	// Wait for entry to be applied on all alive nodes
	appliedCount := 0
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) && appliedCount < 3 {
		appliedCount = 0
		for i := 0; i < 3; i++ {
			select {
			case msg := <-c.applyChs[i]:
				if msg.CommandValid && msg.CommandIndex == index && bytes.Equal(msg.Command, command) {
					appliedCount++
				}
			default:
			}
		}
		if appliedCount < 3 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	if appliedCount < 2 { // Majority must commit
		t.Fatalf("expected majority commit for command, got %d applied nodes", appliedCount)
	}
}
