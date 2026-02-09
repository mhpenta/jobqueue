package jobqueue

import (
	"errors"
	"strings"
)

var (
	ErrEmptyQueueName = errors.New("jobqueue: queue name must not be empty")
	ErrEmptyJobType   = errors.New("jobqueue: job type must not be empty")
	ErrEmptyJobID     = errors.New("jobqueue: job ID must not be empty")
	ErrNilBody        = errors.New("jobqueue: body must not be nil")
	ErrJobNotFound    = errors.New("jobqueue: job not found")
	ErrJobCompleted   = errors.New("jobqueue: job already completed")
	ErrJobNotPending  = errors.New("jobqueue: job not pending")
)

func ValidateEnqueue(queueName, jobType string, body []byte) error {
	if strings.TrimSpace(queueName) == "" {
		return ErrEmptyQueueName
	}
	if strings.TrimSpace(jobType) == "" {
		return ErrEmptyJobType
	}
	if body == nil {
		return ErrNilBody
	}
	return nil
}

func ValidateQueueName(queueName string) error {
	if strings.TrimSpace(queueName) == "" {
		return ErrEmptyQueueName
	}
	return nil
}

func ValidateJobID(jobID string) error {
	if strings.TrimSpace(jobID) == "" {
		return ErrEmptyJobID
	}
	return nil
}

func ResolveEnqueueOptions(opts []EnqueueOption) EnqueueOptions {
	return applyEnqueueOptions(opts)
}
