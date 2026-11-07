package jobqueue

import (
	"encoding/json"
	"time"
)

// Message contains a job's data and metadata from the queue.
// The queue does not interpret Body — consumers deserialize it themselves.
type Message struct {
	ID              string
	JobKey          string
	QueueName       string
	JobType         string
	Body            []byte
	Metadata        json.RawMessage
	Priority        int
	CreatedAt       time.Time
	ClaimedAt       time.Time
	ClaimedBy       string
	CompletedAt     time.Time
	TerminalCode    string
	TerminalSummary string
	ResultJSON      json.RawMessage

	// RetryCount is the total number of recorded failures for this job.
	// It is incremented by Retry, and by Dequeue when the previous claim's
	// visibility timeout ran out without Complete, Retry, or Fail (a worker
	// that crashed or hung counts as a failed attempt).
	//
	// Semantics (max failures):
	//   - First dequeue: RetryCount = 0
	//   - First failure + Retry(): RetryCount = 1
	//   - MaxRetries = 3 allows at most 3 attempts in total
	//   - MaxRetries = 0 still allows the initial attempt
	//
	// A Retry that would use up the last attempt moves the job to the
	// dead-letter queue instead. Admin.SweepStuckJobs moves jobs whose lease
	// expired on their last attempt, and rows left exhausted by versions
	// before v0.5.0.
	RetryCount int

	MaxRetries int
}

// ShouldRetry reports whether Retry would give the job another attempt.
// Use it after a processing failure to decide between Retry and Fail: when it
// returns false, call Fail so the dead-letter queue keeps the real error.
func (m *Message) ShouldRetry() bool {
	return m.RetryCount+1 < m.MaxRetries
}
