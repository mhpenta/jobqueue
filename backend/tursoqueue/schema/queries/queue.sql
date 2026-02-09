-- name: InsertJob :one
INSERT INTO job_queue (
    id, queue_name, job_type, body, priority, visible_after, created_at, retry_count, max_retries
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, 0, ?
)
RETURNING *;

-- name: ClaimJob :one
-- Atomically claim the next available pending job.
UPDATE job_queue
SET visible_after = ?
WHERE id = (
    SELECT jq.id FROM job_queue jq
    WHERE jq.queue_name = ?
      AND jq.completed_at IS NULL
      AND jq.visible_after <= ?
      AND (
        jq.retry_count < jq.max_retries
        OR (jq.max_retries = 0 AND jq.retry_count = 0)
      )
    ORDER BY jq.priority DESC, jq.created_at ASC
    LIMIT 1
)
RETURNING *;

-- name: CompleteJob :execresult
UPDATE job_queue SET completed_at = ? WHERE id = ?;

-- name: RetryJob :execresult
-- Record a failed attempt and make immediately visible.
UPDATE job_queue
SET visible_after = ?, retry_count = retry_count + 1
WHERE id = ? AND completed_at IS NULL;

-- name: GetJob :one
SELECT * FROM job_queue WHERE id = ?;

-- name: GetJobForUpdate :one
SELECT * FROM job_queue WHERE id = ?;

-- name: InsertDLQ :exec
INSERT INTO job_queue_dlq (
    id, queue_name, job_type, body, priority, created_at, failed_at, retry_count, error
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?
);

-- name: DeleteJob :exec
DELETE FROM job_queue WHERE id = ?;

-- name: GetDLQJob :one
SELECT * FROM job_queue_dlq WHERE id = ?;

-- name: DeleteDLQJob :exec
DELETE FROM job_queue_dlq WHERE id = ?;

-- name: ListPendingJobs :many
SELECT * FROM job_queue
WHERE queue_name = ? AND completed_at IS NULL
ORDER BY priority DESC, created_at ASC
LIMIT ? OFFSET ?;

-- name: ListCompletedJobs :many
SELECT * FROM job_queue
WHERE queue_name = ? AND completed_at IS NOT NULL
ORDER BY completed_at DESC
LIMIT ? OFFSET ?;

-- name: ListDLQJobs :many
SELECT * FROM job_queue_dlq
WHERE queue_name = ?
ORDER BY failed_at DESC
LIMIT ? OFFSET ?;

-- name: CleanupCompletedJobs :exec
-- Delete completed jobs older than the given timestamp.
DELETE FROM job_queue WHERE completed_at IS NOT NULL AND completed_at < ?;

-- name: CountPendingJobs :one
SELECT COUNT(*) FROM job_queue WHERE queue_name = ? AND completed_at IS NULL;

-- name: CountCompletedJobs :one
SELECT COUNT(*) FROM job_queue WHERE queue_name = ? AND completed_at IS NOT NULL;

-- name: CountDLQJobs :one
SELECT COUNT(*) FROM job_queue_dlq WHERE queue_name = ?;

-- name: SweepStuckJobs :many
-- Find jobs that exhausted retries but were never completed or moved to DLQ.
SELECT * FROM job_queue
WHERE queue_name = ?
  AND completed_at IS NULL
  AND retry_count >= max_retries;
