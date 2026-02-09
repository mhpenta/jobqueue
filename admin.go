package jobqueue

import (
	"context"
	"time"
)

// Admin provides operational visibility and maintenance for the queue.
// Obtain an Admin from the concrete backend (e.g. turso.New or postgres.New
// both return types that implement both Queue and Admin).
type Admin interface {
	// ListPendingJobs returns uncompleted jobs ordered by priority DESC, created_at ASC.
	ListPendingJobs(ctx context.Context, queueName string, limit, offset int) ([]Message, error)

	// ListCompletedJobs returns completed jobs ordered by completion time DESC.
	ListCompletedJobs(ctx context.Context, queueName string, limit, offset int) ([]Message, error)

	// ListDLQJobs returns dead-letter jobs ordered by failed_at DESC.
	ListDLQJobs(ctx context.Context, queueName string, limit, offset int) ([]DLQMessage, error)

	// CountPendingJobs returns the number of uncompleted jobs in the queue.
	CountPendingJobs(ctx context.Context, queueName string) (int64, error)

	// CountCompletedJobs returns the number of completed jobs still in the table.
	CountCompletedJobs(ctx context.Context, queueName string) (int64, error)

	// CountDLQJobs returns the number of jobs in the dead-letter queue.
	CountDLQJobs(ctx context.Context, queueName string) (int64, error)

	// CleanupCompletedJobs deletes completed jobs older than the given time.
	CleanupCompletedJobs(ctx context.Context, olderThan time.Time) error

	// SweepStuckJobs finds jobs that exhausted their retries (retry_count >= max_retries)
	// but were never completed or moved to DLQ, and moves them to the DLQ.
	// Returns the number of jobs swept.
	SweepStuckJobs(ctx context.Context, queueName string) (int, error)

	// DeleteDLQJob permanently deletes a job from the dead-letter queue.
	DeleteDLQJob(ctx context.Context, jobID string) error
}

// DLQMessage contains a dead-letter job's data and failure metadata.
type DLQMessage struct {
	ID         string
	QueueName  string
	JobType    string
	Body       []byte
	Priority   int
	CreatedAt  time.Time
	FailedAt   time.Time
	RetryCount int
	Error      string
}
