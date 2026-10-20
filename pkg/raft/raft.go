package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/FelipeMadson/go-raft-distributed-consensus-kv/pkg/transport"
)

// Role represents the current state of a Raft node.
type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// ApplyMsg is sent on applyCh whenever a log entry is committed.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int
}

// RaftNode represents an individual peer in the Raft consensus group.
type RaftNode struct {
	mu        sync.Mutex
	peers     []int
	storage   Storage
	network   *transport.Network
	me        int
	dead      bool
	applyCh   chan ApplyMsg

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry // 1-indexed conceptually; index 0 is sentinel

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	role        Role

	// Volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	// Timers and triggers
	lastResetElection time.Time
	electionTimeout   time.Duration
	stopCh            chan struct{}
}

// NewRaftNode creates and initializes a Raft peer.
func NewRaftNode(me int, peers []int, storage Storage, network *transport.Network, applyCh chan ApplyMsg) *RaftNode {
	rn := &RaftNode{
		peers:           peers,
		storage:         storage,
		network:         network,
		me:              me,
		applyCh:         applyCh,
		currentTerm:     0,
		votedFor:        -1,
		log:             []LogEntry{{Index: 0, Term: 0, Command: nil}}, // sentinel at index 0
		commitIndex:     0,
		lastApplied:     0,
		role:            Follower,
		nextIndex:       make(map[int]int),
		matchIndex:      make(map[int]int),
		electionTimeout: randomizeElectionTimeout(),
		stopCh:          make(chan struct{}),
	}

	// Restore persistent state if available
	if storage != nil {
		term, votedFor, entries, err := storage.Load()
		if err == nil && len(entries) > 0 {
			rn.currentTerm = term
			rn.votedFor = votedFor
			rn.log = entries
		}
	}

	rn.lastResetElection = time.Now()

	// Register with transport network
	if network != nil {
		network.Register(me, rn)
	}

	go rn.ticker()

	return rn
}

func randomizeElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// GetState returns currentTerm and whether this server believes it is the leader.
func (rn *RaftNode) GetState() (int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm, rn.role == Leader
}

// Start proposes a command to the Raft cluster. Returns (index, term, isLeader).
func (rn *RaftNode) Start(command []byte) (int, int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != Leader || rn.dead {
		return -1, -1, false
	}

	newIndex := len(rn.log)
	term := rn.currentTerm
	entry := LogEntry{
		Index:   newIndex,
		Term:    term,
		Command: command,
	}

	rn.log = append(rn.log, entry)
	rn.persist()

	// Trigger immediate replication
	go rn.broadcastAppendEntries()

	return newIndex, term, true
}

// Kill stops all background goroutines.
func (rn *RaftNode) Kill() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if !rn.dead {
		rn.dead = true
		close(rn.stopCh)
	}
}

// isDead returns if node has been terminated.
func (rn *RaftNode) isDead() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.dead
}

func (rn *RaftNode) persist() {
	if rn.storage != nil {
		_ = rn.storage.Save(rn.currentTerm, rn.votedFor, rn.log)
	}
}

// HandleRPC dispatches incoming simulated network RPC calls.
func (rn *RaftNode) HandleRPC(method string, args interface{}, reply interface{}) error {
	switch method {
	case "RaftNode.RequestVote":
		req, ok1 := args.(*RequestVoteArgs)
		rep, ok2 := reply.(*RequestVoteReply)
		if !ok1 || !ok2 {
			return errors.New("type assertion failed for RequestVote")
		}
		rn.RequestVote(req, rep)
		return nil
	case "RaftNode.AppendEntries":
		req, ok1 := args.(*AppendEntriesArgs)
		rep, ok2 := reply.(*AppendEntriesReply)
		if !ok1 || !ok2 {
			return errors.New("type assertion failed for AppendEntries")
		}
		rn.AppendEntries(req, rep)
		return nil
	default:
		return fmt.Errorf("unknown rpc method: %s", method)
	}
}

// RequestVote RPC handler
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = -1
		rn.persist()
	}

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	lastLog := rn.log[len(rn.log)-1]
	upToDate := false
	if args.LastLogTerm > lastLog.Term {
		upToDate = true
	} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index {
		upToDate = true
	}

	if (rn.votedFor == -1 || rn.votedFor == args.CandidateID) && upToDate {
		rn.votedFor = args.CandidateID
		rn.persist()
		reply.VoteGranted = true
		rn.lastResetElection = time.Now()
	}
}

// AppendEntries RPC handler
func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Success = false
	reply.Term = rn.currentTerm

	if args.Term < rn.currentTerm {
		return
	}

	if args.Term > rn.currentTerm || rn.role == Candidate {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = -1
		rn.persist()
	}

	rn.lastResetElection = time.Now()

	// Log consistency check
	if args.PrevLogIndex >= len(rn.log) {
		reply.ConflictIndex = len(rn.log)
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex >= 0 && rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rn.log[args.PrevLogIndex].Term
		for i := 0; i <= args.PrevLogIndex; i++ {
			if rn.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	// Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rn.log) {
			if rn.log[idx].Term != entry.Term {
				rn.log = rn.log[:idx]
				rn.log = append(rn.log, entry)
			}
		} else {
			rn.log = append(rn.log, entry)
		}
	}
	rn.persist()

	// Update commitIndex
	if args.LeaderCommit > rn.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
		rn.applyEntries()
	}

	reply.Success = true
}

func (rn *RaftNode) applyEntries() {
	for rn.commitIndex > rn.lastApplied {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
		if rn.applyCh != nil {
			rn.applyCh <- msg
		}
	}
}

func (rn *RaftNode) ticker() {
	for !rn.isDead() {
		time.Sleep(20 * time.Millisecond)

		rn.mu.Lock()
		now := time.Now()
		role := rn.role

		if role == Leader {
			rn.mu.Unlock()
			continue
		}

		if now.Sub(rn.lastResetElection) >= rn.electionTimeout {
			rn.startElection()
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) startElection() {
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.me
	rn.persist()
	rn.lastResetElection = time.Now()
	rn.electionTimeout = randomizeElectionTimeout()

	term := rn.currentTerm
	lastLog := rn.log[len(rn.log)-1]

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rn.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	votes := 1
	var voteMu sync.Mutex
	totalNodes := len(rn.peers) + 1
	needed := totalNodes/2 + 1

	for _, peer := range rn.peers {
		go func(peerID int) {
			var reply RequestVoteReply
			ok := rn.network.Call(rn.me, peerID, "RaftNode.RequestVote", &args, &reply)
			if !ok {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = -1
				rn.persist()
				return
			}

			if rn.role == Candidate && rn.currentTerm == term && reply.VoteGranted {
				voteMu.Lock()
				votes++
				isLeader := votes >= needed
				voteMu.Unlock()

				if isLeader && rn.role == Candidate {
					rn.role = Leader
					for _, p := range rn.peers {
						rn.nextIndex[p] = len(rn.log)
						rn.matchIndex[p] = 0
					}
					go rn.leaderHeartbeatLoop(term)
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) leaderHeartbeatLoop(term int) {
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()

	for !rn.isDead() {
		rn.mu.Lock()
		if rn.role != Leader || rn.currentTerm != term {
			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()

		rn.broadcastAppendEntries()

		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (rn *RaftNode) broadcastAppendEntries() {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return
	}
	term := rn.currentTerm
	commitIdx := rn.commitIndex
	peers := append([]int(nil), rn.peers...)
	rn.mu.Unlock()

	for _, peer := range peers {
		go func(peerID int) {
			rn.mu.Lock()
			if rn.role != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}

			prevIdx := rn.nextIndex[peerID] - 1
			if prevIdx < 0 {
				prevIdx = 0
			}
			prevTerm := rn.log[prevIdx].Term

			var entries []LogEntry
			if len(rn.log) > prevIdx+1 {
				entries = make([]LogEntry, len(rn.log)-(prevIdx+1))
				copy(entries, rn.log[prevIdx+1:])
			}

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     rn.me,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: commitIdx,
			}
			rn.mu.Unlock()

			var reply AppendEntriesReply
			ok := rn.network.Call(rn.me, peerID, "RaftNode.AppendEntries", &args, &reply)
			if !ok {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = -1
				rn.persist()
				return
			}

			if rn.role != Leader || rn.currentTerm != term {
				return
			}

			if reply.Success {
				rn.nextIndex[peerID] = args.PrevLogIndex + len(args.Entries) + 1
				rn.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)

				// Check if we can advance commitIndex
				for N := len(rn.log) - 1; N > rn.commitIndex; N-- {
					if rn.log[N].Term == rn.currentTerm {
						count := 1 // self
						for _, p := range rn.peers {
							if rn.matchIndex[p] >= N {
								count++
							}
						}
						if count >= (len(rn.peers)+1)/2+1 {
							rn.commitIndex = N
							rn.applyEntries()
							break
						}
					}
				}
			} else {
				if reply.ConflictIndex > 0 {
					rn.nextIndex[peerID] = reply.ConflictIndex
				} else if rn.nextIndex[peerID] > 1 {
					rn.nextIndex[peerID]--
				}
			}
		}(peer)
	}
}
