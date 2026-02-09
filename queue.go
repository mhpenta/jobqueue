package jobqueue

import "context"

// Queue is the interface for the job queue.
// It stores typed JSON blobs with priority, retry, and scheduling semantics.
// The queue has no knowledge of what jobs do — consumers interpret the data.
type Queue interface {
	// Enqueue adds a job to the queue. Returns the job ID.
	Enqueue(ctx context.Context, queueName string, jobType string, body []byte, opts ...EnqueueOption) (string, error)

	// Dequeue atomically claims and returns the next available job.
	// Returns nil if no jobs are available.
	Dequeue(ctx context.Context, queueName string) (*Message, error)

	// Complete marks a job as successfully completed.
	Complete(ctx context.Context, jobID string) error

	// Retry releases a job back to the queue for another attempt.
	// The job becomes immediately visible for processing.
	Retry(ctx context.Context, jobID string) error

	// Fail moves a job to the dead letter queue with the given error.
	// Use this when a job has permanently failed.
	Fail(ctx context.Context, jobID string, err error) error
}
