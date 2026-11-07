package postgresqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mhpenta/jobqueue"
	"github.com/mhpenta/jobqueue/backend/postgresqueue/db"
)

var (
	_ jobqueue.Queue       = (*PostgresQueue)(nil)
	_ jobqueue.Admin       = (*PostgresQueue)(nil)
	_ jobqueue.UniqueQueue = (*PostgresQueue)(nil)
	_ jobqueue.WorkerQueue = (*PostgresQueue)(nil)
	_ jobqueue.ResultQueue = (*PostgresQueue)(nil)
	_ jobqueue.LookupQueue = (*PostgresQueue)(nil)
)

type PostgresQueue struct {
	queries *db.Queries
	db      *sql.DB
	config  jobqueue.Config
}

func New(database *sql.DB, config jobqueue.Config) *PostgresQueue {
	return &PostgresQueue{
		queries: db.New(database),
		db:      database,
		config:  config,
	}
}

func (q *PostgresQueue) Enqueue(ctx context.Context, queueName string, jobType string, body []byte, opts ...jobqueue.EnqueueOption) (string, error) {
	id, _, err := q.enqueue(ctx, queueName, jobType, body, false, opts...)
	return id, err
}

func (q *PostgresQueue) EnqueueUnique(ctx context.Context, queueName string, jobType string, body []byte, opts ...jobqueue.EnqueueOption) (string, bool, error) {
	return q.enqueue(ctx, queueName, jobType, body, true, opts...)
}

func (q *PostgresQueue) enqueue(ctx context.Context, queueName string, jobType string, body []byte, unique bool, opts ...jobqueue.EnqueueOption) (string, bool, error) {
	if err := jobqueue.ValidateEnqueue(queueName, jobType, body); err != nil {
		return "", false, err
	}

	options := jobqueue.ResolveEnqueueOptions(opts)
	jobKey := strings.TrimSpace(options.JobKey)
	if unique {
		if err := jobqueue.ValidateJobKey(jobKey); err != nil {
			return "", false, err
		}
	} else if jobKey != "" {
		if err := jobqueue.ValidateJobKey(jobKey); err != nil {
			return "", false, err
		}
	}
	metadata, err := encodeJSONValue(options.Metadata)
	if err != nil {
		return "", false, fmt.Errorf("encode metadata: %w", err)
	}

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

	params := db.InsertJobParams{
		ID:           id,
		JobKey:       jobKey,
		QueueName:    queueName,
		JobType:      jobType,
		Body:         body,
		Metadata:     metadata,
		Priority:     int32(options.Priority),
		VisibleAfter: visibleAfter,
		CreatedAt:    nowUnix,
		MaxRetries:   int32(q.config.MaxRetries),
	}
	if unique {
		rows, err := q.queries.InsertJobUnique(ctx, db.InsertJobUniqueParams(params))
		if err != nil {
			return "", false, fmt.Errorf("failed to insert unique job: %w", err)
		}
		if len(rows) == 0 {
			existing, err := q.GetJobByKey(ctx, queueName, jobKey)
			if err != nil {
				return "", false, fmt.Errorf("load existing unique job: %w", err)
			}
			return existing.ID, false, nil
		}
		return rows[0].ID, true, nil
	}

	row, err := q.queries.InsertJob(ctx, params)
	if err != nil {
		return "", false, fmt.Errorf("failed to insert job: %w", err)
	}

	return row.ID, true, nil
}

func (q *PostgresQueue) Dequeue(ctx context.Context, queueName string) (*jobqueue.Message, error) {
	return q.DequeueWithWorker(ctx, queueName, "")
}

func (q *PostgresQueue) DequeueWithWorker(ctx context.Context, queueName string, workerName string) (*jobqueue.Message, error) {
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
		LeaseUntil: newVisibleAfter,
		ClaimedAt:  sql.NullInt64{Int64: now, Valid: true},
		ClaimedBy:  strings.TrimSpace(workerName),
		QueueName:  queueName,
		Now:        now,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to claim job: %w", err)
	}

	return messageFromJobRow(row), nil
}

func (q *PostgresQueue) Complete(ctx context.Context, jobID string) error {
	return q.complete(ctx, jobID, nil)
}

func (q *PostgresQueue) CompleteWithResult(ctx context.Context, jobID string, result jobqueue.JobResult) error {
	return q.complete(ctx, jobID, &result)
}

func (q *PostgresQueue) complete(ctx context.Context, jobID string, result *jobqueue.JobResult) error {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return err
	}
	now := time.Now().Unix()
	if result == nil {
		res, err := q.queries.CompleteJob(ctx, db.CompleteJobParams{
			CompletedAt: sql.NullInt64{Int64: now, Valid: true},
			ID:          jobID,
		})
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return q.notCompletable(ctx, jobID)
		}
		return nil
	}

	payloadJSON, err := encodeJSONValue(result.Payload)
	if err != nil {
		return fmt.Errorf("encode result payload: %w", err)
	}

	res, err := q.queries.CompleteJobWithResult(ctx, db.CompleteJobWithResultParams{
		CompletedAt:     sql.NullInt64{Int64: now, Valid: true},
		TerminalCode:    strings.TrimSpace(result.Code),
		TerminalSummary: strings.TrimSpace(result.Summary),
		ResultJson:      payloadJSON,
		ID:              jobID,
	})
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return q.notCompletable(ctx, jobID)
	}
	return nil
}

// notCompletable handles a completion that matched no pending row. Completing
// an already-completed job is a no-op that keeps the first completion's time
// and result.
func (q *PostgresQueue) notCompletable(ctx context.Context, jobID string) error {
	job, err := q.queries.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return jobqueue.ErrJobNotFound
		}
		return fmt.Errorf("get job: %w", err)
	}
	if job.CompletedAt.Valid {
		return nil
	}
	return jobqueue.ErrJobNotFound
}

// Retry records a failed attempt and releases the job for another one, after
// Config.RetryBackoff if set. If that attempt would exceed MaxRetries, the job
// moves to the dead-letter queue instead; call Fail directly (see
// Message.ShouldRetry) to keep the processing error there.
func (q *PostgresQueue) Retry(ctx context.Context, jobID string) error {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return err
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
			return jobqueue.ErrJobNotPending
		}
		return fmt.Errorf("failed to get job: %w", err)
	}
	if job.CompletedAt.Valid {
		return jobqueue.ErrJobNotPending
	}

	t := time.Now()
	retryCount := job.RetryCount + 1
	if retryCount >= job.MaxRetries {
		err := moveToDLQ(ctx, qtx, job, dlqEntry{
			retryCount: retryCount,
			errMsg:     "jobqueue: retry limit reached",
			failedAt:   t.Unix(),
			resultJSON: job.ResultJson,
			code:       job.TerminalCode,
			summary:    job.TerminalSummary,
		})
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	visibleAfter := int64(0)
	if delay := q.config.RetryDelay(int(retryCount)); delay > 0 {
		visibleAfter = t.Add(delay).Unix()
		if visibleAfter <= t.Unix() {
			visibleAfter = t.Unix() + 1
		}
	}
	if _, err := qtx.ReleaseJob(ctx, db.ReleaseJobParams{
		VisibleAfter: visibleAfter,
		RetryCount:   retryCount,
		ID:           jobID,
	}); err != nil {
		return fmt.Errorf("failed to release job: %w", err)
	}
	return tx.Commit()
}

func (q *PostgresQueue) Fail(ctx context.Context, jobID string, jobErr error) error {
	return q.FailWithResult(ctx, jobID, jobErr, jobqueue.JobResult{})
}

func (q *PostgresQueue) FailWithResult(ctx context.Context, jobID string, jobErr error, result jobqueue.JobResult) error {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return err
	}

	errMsg := ""
	if jobErr != nil {
		errMsg = jobErr.Error()
	}
	resultJSON, err := encodeJSONValue(result.Payload)
	if err != nil {
		return fmt.Errorf("encode result payload: %w", err)
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

	err = moveToDLQ(ctx, qtx, job, dlqEntry{
		retryCount: job.RetryCount,
		errMsg:     errMsg,
		failedAt:   time.Now().Unix(),
		resultJSON: resultJSON,
		code:       strings.TrimSpace(result.Code),
		summary:    strings.TrimSpace(result.Summary),
	})
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (q *PostgresQueue) GetJob(ctx context.Context, jobID string) (*jobqueue.Message, error) {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return nil, err
	}
	row, err := q.queries.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, jobqueue.ErrJobNotFound
		}
		return nil, fmt.Errorf("get job: %w", err)
	}
	return messageFromJobRow(row), nil
}

func (q *PostgresQueue) GetJobByKey(ctx context.Context, queueName string, jobKey string) (*jobqueue.Message, error) {
	if err := jobqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	if err := jobqueue.ValidateJobKey(jobKey); err != nil {
		return nil, err
	}
	row, err := q.queries.GetJobByKey(ctx, db.GetJobByKeyParams{
		QueueName: queueName,
		JobKey:    strings.TrimSpace(jobKey),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, jobqueue.ErrJobNotFound
		}
		return nil, fmt.Errorf("get job by key: %w", err)
	}
	return messageFromJobRow(row), nil
}

func (q *PostgresQueue) GetDLQJob(ctx context.Context, jobID string) (*jobqueue.DLQMessage, error) {
	if err := jobqueue.ValidateJobID(jobID); err != nil {
		return nil, err
	}
	row, err := q.queries.GetDLQJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, jobqueue.ErrJobNotFound
		}
		return nil, fmt.Errorf("get dlq job: %w", err)
	}
	return dlqMessageFromRow(row), nil
}

func (q *PostgresQueue) GetDLQJobByKey(ctx context.Context, queueName string, jobKey string) (*jobqueue.DLQMessage, error) {
	if err := jobqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	if err := jobqueue.ValidateJobKey(jobKey); err != nil {
		return nil, err
	}
	row, err := q.queries.GetDLQJobByKey(ctx, db.GetDLQJobByKeyParams{
		QueueName: queueName,
		JobKey:    strings.TrimSpace(jobKey),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, jobqueue.ErrJobNotFound
		}
		return nil, fmt.Errorf("get dlq job by key: %w", err)
	}
	return dlqMessageFromRow(row), nil
}

// dlqEntry holds the dead-letter fields that differ between Fail, Retry, and
// SweepStuckJobs.
type dlqEntry struct {
	retryCount int32
	errMsg     string
	failedAt   int64
	resultJSON json.RawMessage
	code       string
	summary    string
}

// moveToDLQ copies job into the dead-letter queue and deletes it from
// job_queue. Callers run it inside a transaction.
func moveToDLQ(ctx context.Context, qtx *db.Queries, job db.JobQueue, e dlqEntry) error {
	err := qtx.InsertDLQ(ctx, db.InsertDLQParams{
		ID:              job.ID,
		JobKey:          job.JobKey,
		QueueName:       job.QueueName,
		JobType:         job.JobType,
		Body:            job.Body,
		Metadata:        job.Metadata,
		Priority:        job.Priority,
		CreatedAt:       job.CreatedAt,
		FailedAt:        e.failedAt,
		ClaimedAt:       job.ClaimedAt,
		ClaimedBy:       job.ClaimedBy,
		RetryCount:      e.retryCount,
		Error:           e.errMsg,
		TerminalCode:    e.code,
		TerminalSummary: e.summary,
		ResultJson:      e.resultJSON,
	})
	if err != nil {
		return fmt.Errorf("failed to insert job %s into DLQ: %w", job.ID, err)
	}
	if err := qtx.DeleteJob(ctx, job.ID); err != nil {
		return fmt.Errorf("failed to delete job %s: %w", job.ID, err)
	}
	return nil
}

// leaseExpired reports whether job was claimed and its lease ran out before
// Complete, Retry, or Fail. It mirrors the ClaimJob and SweepStuckJobs SQL.
func leaseExpired(job db.JobQueue, now int64) bool {
	return job.ClaimedAt.Valid && job.VisibleAfter > 0 && job.VisibleAfter <= now
}

func encodeJSONValue(value any) (json.RawMessage, error) {
	switch v := value.(type) {
	case nil:
		return json.RawMessage(`{}`), nil
	case json.RawMessage:
		if len(v) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return append(json.RawMessage(nil), v...), nil
	case []byte:
		if len(v) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return append(json.RawMessage(nil), v...), nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(raw), nil
	}
}

func nullUnixTime(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.Unix(value.Int64, 0)
}

func messageFromJobRow(row db.JobQueue) *jobqueue.Message {
	return &jobqueue.Message{
		ID:              row.ID,
		JobKey:          row.JobKey,
		QueueName:       row.QueueName,
		JobType:         row.JobType,
		Body:            row.Body,
		Metadata:        append(json.RawMessage(nil), row.Metadata...),
		Priority:        int(row.Priority),
		CreatedAt:       time.Unix(row.CreatedAt, 0),
		ClaimedAt:       nullUnixTime(row.ClaimedAt),
		ClaimedBy:       row.ClaimedBy,
		CompletedAt:     nullUnixTime(row.CompletedAt),
		TerminalCode:    row.TerminalCode,
		TerminalSummary: row.TerminalSummary,
		ResultJSON:      append(json.RawMessage(nil), row.ResultJson...),
		RetryCount:      int(row.RetryCount),
		MaxRetries:      int(row.MaxRetries),
	}
}

func dlqMessageFromRow(row db.JobQueueDlq) *jobqueue.DLQMessage {
	return &jobqueue.DLQMessage{
		ID:              row.ID,
		JobKey:          row.JobKey,
		QueueName:       row.QueueName,
		JobType:         row.JobType,
		Body:            row.Body,
		Metadata:        append(json.RawMessage(nil), row.Metadata...),
		Priority:        int(row.Priority),
		CreatedAt:       time.Unix(row.CreatedAt, 0),
		FailedAt:        time.Unix(row.FailedAt, 0),
		ClaimedAt:       nullUnixTime(row.ClaimedAt),
		ClaimedBy:       row.ClaimedBy,
		RetryCount:      int(row.RetryCount),
		Error:           row.Error,
		TerminalCode:    row.TerminalCode,
		TerminalSummary: row.TerminalSummary,
		ResultJSON:      append(json.RawMessage(nil), row.ResultJson...),
	}
}
