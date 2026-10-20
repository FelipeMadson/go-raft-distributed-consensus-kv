package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/raft"
)

// OpType represents the mutation or query operation.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpGet    OpType = "GET"
	OpDelete OpType = "DELETE"
)

// Op represents the operation payload replicated through Raft.
type Op struct {
	Type     OpType `json:"type"`
	Key      string `json:"key"`
	Value    string `json:"value"`
	ClientID string `json:"client_id"`
	SeqNum   int64  `json:"seq_num"`
}

// OpResult holds the result of an executed operation.
type OpResult struct {
	Value  string
	Exists bool
	Err    error
}

// KVServer represents the replicated linearizable Key-Value store node.
type KVServer struct {
	mu             sync.RWMutex
	me             int
	rf             *raft.RaftNode
	applyCh        chan raft.ApplyMsg
	dead           bool
	data           map[string]string
	lastAppliedSeq map[string]int64
	waitNotify     map[int]chan OpResult
	stopCh         chan struct{}
}

// NewKVServer creates and starts a Key-Value server over a Raft node.
func NewKVServer(me int, rf *raft.RaftNode, applyCh chan raft.ApplyMsg) *KVServer {
	kv := &KVServer{
		me:             me,
		rf:             rf,
		applyCh:        applyCh,
		data:           make(map[string]string),
		lastAppliedSeq: make(map[string]int64),
		waitNotify:     make(map[int]chan OpResult),
		stopCh:         make(chan struct{}),
	}

	go kv.applyLoop()
	return kv
}

// Close shuts down the KV server and its underlying Raft node.
func (kv *KVServer) Close() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.dead {
		kv.dead = true
		close(kv.stopCh)
		kv.rf.Kill()
	}
}

// Put writes or updates a key-value pair.
func (kv *KVServer) Put(clientID string, seqNum int64, key, value string) error {
	op := Op{
		Type:     OpPut,
		Key:      key,
		Value:    value,
		ClientID: clientID,
		SeqNum:   seqNum,
	}
	_, err := kv.executeOp(op)
	return err
}

// Get reads the value associated with a key with linearizable guarantees.
func (kv *KVServer) Get(key string) (string, bool, error) {
	op := Op{
		Type:     OpGet,
		Key:      key,
		ClientID: fmt.Sprintf("read-%d", time.Now().UnixNano()),
		SeqNum:   time.Now().UnixNano(),
	}
	res, err := kv.executeOp(op)
	if err != nil {
		return "", false, err
	}
	return res.Value, res.Exists, nil
}

// Delete removes a key-value pair.
func (kv *KVServer) Delete(clientID string, seqNum int64, key string) error {
	op := Op{
		Type:     OpDelete,
		Key:      key,
		ClientID: clientID,
		SeqNum:   seqNum,
	}
	_, err := kv.executeOp(op)
	return err
}

func (kv *KVServer) executeOp(op Op) (OpResult, error) {
	bytes, err := json.Marshal(op)
	if err != nil {
		return OpResult{}, err
	}

	index, _, isLeader := kv.rf.Start(bytes)
	if !isLeader {
		return OpResult{}, errors.New("not leader")
	}

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitNotify[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return res, res.Err
	case <-time.After(2 * time.Second):
		kv.mu.Lock()
		delete(kv.waitNotify, index)
		kv.mu.Unlock()
		return OpResult{}, errors.New("operation timeout")
	}
}

func (kv *KVServer) applyLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg, ok := <-kv.applyCh:
			if !ok {
				return
			}
			if !msg.CommandValid {
				continue
			}

			var op Op
			if err := json.Unmarshal(msg.Command, &op); err != nil {
				continue
			}

			kv.mu.Lock()
			res := OpResult{}

			// Check client deduplication for mutations
			lastSeq, seen := kv.lastAppliedSeq[op.ClientID]
			isDuplicate := seen && op.SeqNum <= lastSeq

			switch op.Type {
			case OpPut:
				if !isDuplicate {
					kv.data[op.Key] = op.Value
					if op.ClientID != "" {
						kv.lastAppliedSeq[op.ClientID] = op.SeqNum
					}
				}
				res.Value = op.Value
				res.Exists = true
			case OpDelete:
				if !isDuplicate {
					delete(kv.data, op.Key)
					if op.ClientID != "" {
						kv.lastAppliedSeq[op.ClientID] = op.SeqNum
					}
				}
				res.Exists = false
			case OpGet:
				val, exists := kv.data[op.Key]
				res.Value = val
				res.Exists = exists
			}

			// Notify waiting caller if present
			if ch, exists := kv.waitNotify[msg.CommandIndex]; exists {
				ch <- res
				delete(kv.waitNotify, msg.CommandIndex)
			}

			kv.mu.Unlock()
		}
	}
}

// DumpState returns a snapshot copy of the key-value map for diagnostics.
func (kv *KVServer) DumpState() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	copyMap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		copyMap[k] = v
	}
	return copyMap
}
