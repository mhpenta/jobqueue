package jobqueue

import "time"

// Message contains a job's data and metadata from the queue.
// The queue does not interpret Body — consumers deserialize it themselves.
type Message struct {
	ID        string
	QueueName string
	JobType   string
	Body      []byte
	Priority  int
	CreatedAt time.Time

	// RetryCount is the total number of recorded failures for this job.
	// It is incremented by Retry (and other failure paths), NOT by Dequeue.
	//
	// Semantics (max failures):
	//   - First dequeue succeeds: RetryCount = 0
	//   - First failure + Retry(): RetryCount = 1
	//   - Subsequent failures increment each time Retry() is called
	//   - MaxRetries = 3 means the job can be retried (failed) at most 3 times
	//
	// Jobs that exhaust all retries (RetryCount >= MaxRetries) become invisible
	// to Dequeue but remain in the job_queue table. The only exception is
	// MaxRetries = 0, which still allows the initial attempt (RetryCount = 0).
	// Use Admin.SweepStuckJobs to move exhausted jobs to the DLQ.
	RetryCount int

	MaxRetries int
}

// ShouldRetry returns true if the job has failure attempts remaining.
// Use this after a processing failure to decide between Retry and Fail.
func (m *Message) ShouldRetry() bool {
	return m.RetryCount < m.MaxRetries
}
