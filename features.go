package jobqueue

import "context"

// JobResult records an optional terminal outcome and structured result payload.
type JobResult struct {
	Code    string
	Summary string
	Payload any
}

// UniqueQueue adds idempotent enqueue support keyed by queue_name + job_key.
type UniqueQueue interface {
	EnqueueUnique(ctx context.Context, queueName string, jobType string, body []byte, opts ...EnqueueOption) (jobID string, created bool, err error)
}

// WorkerQueue records worker identity when claiming jobs.
type WorkerQueue interface {
	DequeueWithWorker(ctx context.Context, queueName string, workerName string) (*Message, error)
}

// ResultQueue records terminal result metadata alongside completion/failure.
type ResultQueue interface {
	CompleteWithResult(ctx context.Context, jobID string, result JobResult) error
	FailWithResult(ctx context.Context, jobID string, err error, result JobResult) error
}

// LookupQueue exposes direct lookup helpers for queue records.
type LookupQueue interface {
	GetJob(ctx context.Context, jobID string) (*Message, error)
	GetJobByKey(ctx context.Context, queueName string, jobKey string) (*Message, error)
	GetDLQJob(ctx context.Context, jobID string) (*DLQMessage, error)
	GetDLQJobByKey(ctx context.Context, queueName string, jobKey string) (*DLQMessage, error)
}

func EnqueueUnique(ctx context.Context, queue Queue, queueName string, jobType string, body []byte, opts ...EnqueueOption) (jobID string, created bool, err error) {
	if uq, ok := queue.(UniqueQueue); ok {
		return uq.EnqueueUnique(ctx, queueName, jobType, body, opts...)
	}
	return "", false, ErrUnsupported
}

func DequeueWithWorker(ctx context.Context, queue Queue, queueName string, workerName string) (*Message, error) {
	if wq, ok := queue.(WorkerQueue); ok {
		return wq.DequeueWithWorker(ctx, queueName, workerName)
	}
	return queue.Dequeue(ctx, queueName)
}

func CompleteWithResult(ctx context.Context, queue Queue, jobID string, result JobResult) error {
	if rq, ok := queue.(ResultQueue); ok {
		return rq.CompleteWithResult(ctx, jobID, result)
	}
	return queue.Complete(ctx, jobID)
}

func FailWithResult(ctx context.Context, queue Queue, jobID string, err error, result JobResult) error {
	if rq, ok := queue.(ResultQueue); ok {
		return rq.FailWithResult(ctx, jobID, err, result)
	}
	return queue.Fail(ctx, jobID, err)
}

func GetJob(ctx context.Context, queue Queue, jobID string) (*Message, error) {
	lq, ok := queue.(LookupQueue)
	if !ok {
		return nil, ErrUnsupported
	}
	return lq.GetJob(ctx, jobID)
}

func GetJobByKey(ctx context.Context, queue Queue, queueName string, jobKey string) (*Message, error) {
	lq, ok := queue.(LookupQueue)
	if !ok {
		return nil, ErrUnsupported
	}
	return lq.GetJobByKey(ctx, queueName, jobKey)
}

func GetDLQJob(ctx context.Context, queue Queue, jobID string) (*DLQMessage, error) {
	lq, ok := queue.(LookupQueue)
	if !ok {
		return nil, ErrUnsupported
	}
	return lq.GetDLQJob(ctx, jobID)
}

func GetDLQJobByKey(ctx context.Context, queue Queue, queueName string, jobKey string) (*DLQMessage, error) {
	lq, ok := queue.(LookupQueue)
	if !ok {
		return nil, ErrUnsupported
	}
	return lq.GetDLQJobByKey(ctx, queueName, jobKey)
}
