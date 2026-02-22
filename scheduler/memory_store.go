package scheduler

import (
	"context"
	"strings"
	"sync"
	"time"
)

// MemoryStore is an in-process key state store.
// It is safe for concurrent use within one process.
type MemoryStore struct {
	mu    sync.Mutex
	state map[string]time.Time
}

// NewMemoryStore creates an empty in-memory key state store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		state: make(map[string]time.Time),
	}
}

// Update atomically computes and stores the next scheduled time for a key.
func (m *MemoryStore) Update(ctx context.Context, key string, fn func(previous time.Time, exists bool) (next time.Time, err error)) (time.Time, error) {
	_ = ctx

	if strings.TrimSpace(key) == "" {
		return time.Time{}, ErrEmptyJobKey
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	previous, exists := m.state[key]
	next, err := fn(previous, exists)
	if err != nil {
		return time.Time{}, err
	}
	if next.IsZero() {
		return time.Time{}, ErrZeroScheduledAt
	}

	m.state[key] = next
	return next, nil
}
