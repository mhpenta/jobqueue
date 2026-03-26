package jobqueue

import "time"

// Config holds queue configuration.
type Config struct {
	// MaxRetries is the default max retry attempts before moving to DLQ.
	MaxRetries int

	// VisibilityTimeout is how long a dequeued job stays invisible.
	// If not acknowledged within this time, it becomes visible again.
	VisibilityTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		MaxRetries:        3,
		VisibilityTimeout: 5 * time.Minute,
	}
}

// EnqueueOptions configures how a job is enqueued.
type EnqueueOptions struct {
	// Priority for this job (higher = processed sooner).
	Priority int

	// RunAt schedules the job to become visible at a specific time.
	// Zero value means immediately visible.
	RunAt time.Time

	// JobKey is an optional caller-supplied idempotency key.
	JobKey string

	// Metadata is optional structured JSON data persisted alongside the job.
	Metadata any
}

// EnqueueOption is a functional option for Enqueue.
type EnqueueOption func(*EnqueueOptions)

// WithPriority sets the job priority (higher = processed sooner).
func WithPriority(priority int) EnqueueOption {
	return func(o *EnqueueOptions) {
		o.Priority = priority
	}
}

// WithDelay delays the job by the given duration from now.
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *EnqueueOptions) {
		o.RunAt = time.Now().Add(d)
	}
}

// WithRunAt schedules the job to become visible at a specific time.
func WithRunAt(t time.Time) EnqueueOption {
	return func(o *EnqueueOptions) {
		o.RunAt = t
	}
}

// WithJobKey sets an optional idempotency key for the job.
func WithJobKey(jobKey string) EnqueueOption {
	return func(o *EnqueueOptions) {
		o.JobKey = jobKey
	}
}

// WithMetadata attaches optional structured metadata to the job.
func WithMetadata(metadata any) EnqueueOption {
	return func(o *EnqueueOptions) {
		o.Metadata = metadata
	}
}

// applyEnqueueOptions applies options and returns the result.
func applyEnqueueOptions(opts []EnqueueOption) EnqueueOptions {
	var options EnqueueOptions
	for _, opt := range opts {
		opt(&options)
	}
	return options
}
