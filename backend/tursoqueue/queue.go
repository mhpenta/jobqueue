package tursoqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mhpenta/jobqueue"
	"github.com/mhpenta/jobqueue/backend/tursoqueue/db"
)

var (
	_ jobqueue.Queue = (*TursoQueue)(nil)
	_ jobqueue.Admin = (*TursoQueue)(nil)
)

type TursoQueue struct {
	queries *db.Queries
	db      *sql.DB
	config  jobqueue.Config
}

func New(database *sql.DB, config jobqueue.Config) *TursoQueue {
	return &TursoQueue{
		queries: db.New(database),
		db:      database,
		config:  config,
	}
}

func (q *TursoQueue) Enqueue(ctx context.Context, queueName string, jobType string, body []byte, opts ...jobqueue.EnqueueOption) (string, error) {
	if err := jobqueue.ValidateEnqueue(queueName, jobType, body); err != nil {
		return "", err
	}

	options := jobqueue.ResolveEnqueueOptions(opts)

	id := uuid.New().String()
	now := time.Now()
	nowUnix := now.Unix()
	visibleAfter := int64(0)
	if !options.RunAt.IsZero() {
		if options.RunAt.After(now) {
			visibleAfter = options.RunAt.Unix()
			if visibleAfter <= nowUnix {
				visibleAfter = nowUnix + 1
			}
		}
	}

	_, err := q.queries.InsertJob(ctx, db.InsertJobParams{
		ID:           id,
		QueueName:    queueName,
		JobType:      jobType,
		Body:         body,
		Priority:     int64(options.Priority),
		VisibleAfter: visibleAfter,
		CreatedAt:    nowUnix,
		MaxRetries:   int64(q.config.MaxRetries),
	})
	if err != nil {
		return "", fmt.Errorf("failed to insert job: %w", err)
	}

	return id, nil
}

func (q *TursoQueue) Dequeue(ctx context.Context, queueName string) (*jobqueue.Message, error) {
	if err := jobqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}

	t := time.Now()
	now := t.Unix()
	newVisibleAfter := t.Add(q.config.VisibilityTimeout).Unix()
	if q.config.VisibilityTimeout > 0 && newVisibleAfter <= now {
		newVisibleAfter = now + 1
	}

	row, err := q.queries.ClaimJob(ctx, db.ClaimJobParams{
		VisibleAfter:   newVisibleAfter,
		QueueName:      queueName,
		VisibleAfter_2: now,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to claim job: %w", err)
	}

	return &jobqueue.Message{
		ID:         row.ID,
		QueueName:  row.QueueName,
		JobType:    row.JobType,
		Body:       row.Body,
		Priority:   int(row.Priority),
		CreatedAt:  time.Unix(row.CreatedAt, 0),
		RetryCount: int(row.RetryCount),
		MaxRetries: int(row.MaxRetries),
	}, nil
}

func (q *TursoQueue) Complete(ctx context.Context, jobID string) error {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return err
	}
	now := time.Now().Unix()
	result, err := q.queries.CompleteJob(ctx, db.CompleteJobParams{
		CompletedAt: sql.NullInt64{Int64: now, Valid: true},
		ID:          jobID,
	})
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return jobqueue.ErrJobNotFound
	}
	return nil
}

func (q *TursoQueue) Retry(ctx context.Context, jobID string) error {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return err
	}
	result, err := q.queries.RetryJob(ctx, db.RetryJobParams{
		VisibleAfter: 0,
		ID:           jobID,
	})
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return jobqueue.ErrJobNotPending
	}
	return nil
}

func (q *TursoQueue) Fail(ctx context.Context, jobID string, jobErr error) error {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return err
	}

	errMsg := ""
	if jobErr != nil {
		errMsg = jobErr.Error()
	}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	qtx := q.queries.WithTx(tx)

	job, err := qtx.GetJobForUpdate(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return jobqueue.ErrJobNotFound
		}
		return fmt.Errorf("failed to get job: %w", err)
	}
	if job.CompletedAt.Valid {
		return jobqueue.ErrJobCompleted
	}

	now := time.Now().Unix()

	err = qtx.InsertDLQ(ctx, db.InsertDLQParams{
		ID:         job.ID,
		QueueName:  job.QueueName,
		JobType:    job.JobType,
		Body:       job.Body,
		Priority:   job.Priority,
		CreatedAt:  job.CreatedAt,
		FailedAt:   now,
		RetryCount: job.RetryCount,
		Error:      errMsg,
	})
	if err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	if err := qtx.DeleteJob(ctx, jobID); err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}

	return tx.Commit()
}
