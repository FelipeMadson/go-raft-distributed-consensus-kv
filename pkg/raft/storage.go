package raft

import (
	"sync"
)

// Storage defines the interface for durable Raft state persistence.
type Storage interface {
	Save(term int, votedFor int, entries []LogEntry) error
	Load() (term int, votedFor int, entries []LogEntry, err error)
}

// MemoryStorage is an in-memory thread-safe implementation of Storage.
type MemoryStorage struct {
	mu       sync.RWMutex
	term     int
	votedFor int
	entries  []LogEntry
}

// NewMemoryStorage creates a new in-memory Raft storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		term:     0,
		votedFor: -1,
		entries:  make([]LogEntry, 0),
	}
}

// Save atomically persists term, votedFor and log entries.
func (ms *MemoryStorage) Save(term int, votedFor int, entries []LogEntry) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.term = term
	ms.votedFor = votedFor
	ms.entries = make([]LogEntry, len(entries))
	copy(ms.entries, entries)
	return nil
}

// Load retrieves persisted term, votedFor and log entries.
func (ms *MemoryStorage) Load() (int, int, []LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	entriesCopy := make([]LogEntry, len(ms.entries))
	copy(entriesCopy, ms.entries)
	return ms.term, ms.votedFor, entriesCopy, nil
}
