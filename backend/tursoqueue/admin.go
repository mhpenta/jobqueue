package tursoqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mhpenta/jobqueue"
	"github.com/mhpenta/jobqueue/backend/tursoqueue/db"
)

// ListPendingJobs returns uncompleted jobs ordered by priority DESC, created_at ASC.
func (q *TursoQueue) ListPendingJobs(ctx context.Context, queueName string, limit, offset int) ([]jobqueue.Message, error) {
	rows, err := q.queries.ListPendingJobs(ctx, db.ListPendingJobsParams{
		QueueName: queueName,
		Limit:     int64(limit),
		Offset:    int64(offset),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pending jobs: %w", err)
	}
	return convertJobRows(rows), nil
}

// ListCompletedJobs returns completed jobs ordered by completion time DESC.
func (q *TursoQueue) ListCompletedJobs(ctx context.Context, queueName string, limit, offset int) ([]jobqueue.Message, error) {
	rows, err := q.queries.ListCompletedJobs(ctx, db.ListCompletedJobsParams{
		QueueName: queueName,
		Limit:     int64(limit),
		Offset:    int64(offset),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list completed jobs: %w", err)
	}
	return convertJobRows(rows), nil
}

// ListDLQJobs returns dead-letter jobs ordered by failed_at DESC.
func (q *TursoQueue) ListDLQJobs(ctx context.Context, queueName string, limit, offset int) ([]jobqueue.DLQMessage, error) {
	rows, err := q.queries.ListDLQJobs(ctx, db.ListDLQJobsParams{
		QueueName: queueName,
		Limit:     int64(limit),
		Offset:    int64(offset),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list DLQ jobs: %w", err)
	}
	msgs := make([]jobqueue.DLQMessage, len(rows))
	for i, row := range rows {
		msgs[i] = jobqueue.DLQMessage{
			ID:         row.ID,
			QueueName:  row.QueueName,
			JobType:    row.JobType,
			Body:       row.Body,
			Priority:   int(row.Priority),
			CreatedAt:  time.Unix(row.CreatedAt, 0),
			FailedAt:   time.Unix(row.FailedAt, 0),
			RetryCount: int(row.RetryCount),
			Error:      row.Error,
		}
	}
	return msgs, nil
}

// CountPendingJobs returns the number of uncompleted jobs in the queue.
func (q *TursoQueue) CountPendingJobs(ctx context.Context, queueName string) (int64, error) {
	return q.queries.CountPendingJobs(ctx, queueName)
}

// CountCompletedJobs returns the number of completed jobs still in the table.
func (q *TursoQueue) CountCompletedJobs(ctx context.Context, queueName string) (int64, error) {
	return q.queries.CountCompletedJobs(ctx, queueName)
}

// CountDLQJobs returns the number of jobs in the dead-letter queue.
func (q *TursoQueue) CountDLQJobs(ctx context.Context, queueName string) (int64, error) {
	return q.queries.CountDLQJobs(ctx, queueName)
}

// CleanupCompletedJobs deletes completed jobs older than the given time.
func (q *TursoQueue) CleanupCompletedJobs(ctx context.Context, olderThan time.Time) error {
	return q.queries.CleanupCompletedJobs(ctx, sql.NullInt64{
		Int64: olderThan.Unix(),
		Valid: true,
	})
}

// SweepStuckJobs finds jobs that exhausted their retries but were never completed
// or moved to DLQ, and moves each to the DLQ. Returns the number swept.
func (q *TursoQueue) SweepStuckJobs(ctx context.Context, queueName string) (int, error) {
	stuck, err := q.queries.SweepStuckJobs(ctx, queueName)
	if err != nil {
		return 0, fmt.Errorf("failed to find stuck jobs: %w", err)
	}
	if len(stuck) == 0 {
		return 0, nil
	}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	qtx := q.queries.WithTx(tx)
	now := time.Now().Unix()

	for _, job := range stuck {
		err = qtx.InsertDLQ(ctx, db.InsertDLQParams{
			ID:         job.ID,
			QueueName:  job.QueueName,
			JobType:    job.JobType,
			Body:       job.Body,
			Priority:   job.Priority,
			CreatedAt:  job.CreatedAt,
			FailedAt:   now,
			RetryCount: job.RetryCount,
			Error:      "swept: retry count exhausted",
		})
		if err != nil {
			return 0, fmt.Errorf("failed to insert swept job %s into DLQ: %w", job.ID, err)
		}
		if err := qtx.DeleteJob(ctx, job.ID); err != nil {
			return 0, fmt.Errorf("failed to delete swept job %s: %w", job.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit sweep: %w", err)
	}
	return len(stuck), nil
}

// DeleteDLQJob permanently deletes a job from the dead-letter queue.
func (q *TursoQueue) DeleteDLQJob(ctx context.Context, jobID string) error {
	return q.queries.DeleteDLQJob(ctx, jobID)
}

func convertJobRows(rows []db.JobQueue) []jobqueue.Message {
	msgs := make([]jobqueue.Message, len(rows))
	for i, row := range rows {
		msgs[i] = jobqueue.Message{
			ID:         row.ID,
			QueueName:  row.QueueName,
			JobType:    row.JobType,
			Body:       row.Body,
			Priority:   int(row.Priority),
			CreatedAt:  time.Unix(row.CreatedAt, 0),
			RetryCount: int(row.RetryCount),
			MaxRetries: int(row.MaxRetries),
		}
	}
	return msgs
}
