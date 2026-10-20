package raft

// LogEntry represents an entry stored in the Raft replicated log.
type LogEntry struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command []byte `json:"command"`
}

// RequestVoteArgs is sent by candidates to gather votes.
type RequestVoteArgs struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidate_id"`
	LastLogIndex int `json:"last_log_index"`
	LastLogTerm  int `json:"last_log_term"`
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs is invoked by leader to replicate log entries and send heartbeats.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term          int  `json:"term"`
	Success       bool `json:"success"`
	ConflictIndex int  `json:"conflict_index"`
	ConflictTerm  int  `json:"conflict_term"`
}
