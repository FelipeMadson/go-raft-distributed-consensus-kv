package transport

import (
	"errors"
	"sync"
)

// Server defines the RPC receiver endpoints for Raft.
type Server interface {
	HandleRPC(method string, args interface{}, reply interface{}) error
}

// Network simulates a distributed network with support for partitions and latency.
type Network struct {
	mu         sync.RWMutex
	servers    map[int]Server
	isolated   map[int]bool
	partitions map[int]int // nodeID -> partitionGroupID
}

// NewNetwork creates a new simulated network.
func NewNetwork() *Network {
	return &Network{
		servers:    make(map[int]Server),
		isolated:   make(map[int]bool),
		partitions: make(map[int]int),
	}
}

// Register adds a node server to the network.
func (n *Network) Register(id int, s Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[id] = s
}

// IsConnected returns whether two nodes can communicate.
func (n *Network) IsConnected(from, to int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.isolated[from] || n.isolated[to] {
		return false
	}

	pFrom, hasFrom := n.partitions[from]
	pTo, hasTo := n.partitions[to]

	if hasFrom && hasTo && pFrom != pTo {
		return false
	}

	return true
}

// Call simulates an RPC invocation over the network.
func (n *Network) Call(from, to int, method string, args interface{}, reply interface{}) bool {
	if !n.IsConnected(from, to) {
		return false
	}

	n.mu.RLock()
	target, exists := n.servers[to]
	n.mu.RUnlock()

	if !exists {
		return false
	}

	err := target.HandleRPC(method, args, reply)
	return err == nil
}

// Disconnect isolates a single node from the network.
func (n *Network) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isolated[id] = true
}

// Connect restores connectivity to an isolated node.
func (n *Network) Connect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.isolated, id)
}

// Partition divides the nodes into disjoint subsets.
func (n *Network) Partition(subsets [][]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions = make(map[int]int)
	for groupIdx, group := range subsets {
		for _, nodeID := range group {
			n.partitions[nodeID] = groupIdx
		}
	}
}

// Heal removes all network partitions and re-connects all nodes.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isolated = make(map[int]bool)
	n.partitions = make(map[int]int)
}

// ErrRPCFailed is returned when an RPC fails.
var ErrRPCFailed = errors.New("rpc call failed or dropped")
