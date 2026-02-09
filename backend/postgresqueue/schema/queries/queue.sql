-- name: InsertJob :one
INSERT INTO job_queue (
    id, queue_name, job_type, body, priority, visible_after, created_at, retry_count, max_retries
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, 0, $8
)
RETURNING *;

-- name: ClaimJob :one
-- Atomically claim the next available pending job.
UPDATE job_queue
SET visible_after = $1
WHERE id = (
    SELECT jq.id FROM job_queue jq
    WHERE jq.queue_name = $2
      AND jq.completed_at IS NULL
      AND jq.visible_after <= $3
      AND (
        jq.retry_count < jq.max_retries
        OR (jq.max_retries = 0 AND jq.retry_count = 0)
      )
    ORDER BY jq.priority DESC, jq.created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: CompleteJob :execresult
UPDATE job_queue SET completed_at = $1 WHERE id = $2;

-- name: RetryJob :execresult
-- Record a failed attempt and make immediately visible.
UPDATE job_queue
SET visible_after = $1, retry_count = retry_count + 1
WHERE id = $2 AND completed_at IS NULL;

-- name: GetJob :one
SELECT * FROM job_queue WHERE id = $1;

-- name: GetJobForUpdate :one
SELECT * FROM job_queue WHERE id = $1 FOR UPDATE;

-- name: InsertDLQ :exec
INSERT INTO job_queue_dlq (
    id, queue_name, job_type, body, priority, created_at, failed_at, retry_count, error
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
);

-- name: DeleteJob :exec
DELETE FROM job_queue WHERE id = $1;

-- name: GetDLQJob :one
SELECT * FROM job_queue_dlq WHERE id = $1;

-- name: DeleteDLQJob :exec
DELETE FROM job_queue_dlq WHERE id = $1;

-- name: ListPendingJobs :many
SELECT * FROM job_queue
WHERE queue_name = $1 AND completed_at IS NULL
ORDER BY priority DESC, created_at ASC
LIMIT $2 OFFSET $3;

-- name: ListCompletedJobs :many
SELECT * FROM job_queue
WHERE queue_name = $1 AND completed_at IS NOT NULL
ORDER BY completed_at DESC
LIMIT $2 OFFSET $3;

-- name: ListDLQJobs :many
SELECT * FROM job_queue_dlq
WHERE queue_name = $1
ORDER BY failed_at DESC
LIMIT $2 OFFSET $3;

-- name: CleanupCompletedJobs :exec
-- Delete completed jobs older than the given timestamp.
DELETE FROM job_queue WHERE completed_at IS NOT NULL AND completed_at < $1;

-- name: CountPendingJobs :one
SELECT COUNT(*) FROM job_queue WHERE queue_name = $1 AND completed_at IS NULL;

-- name: CountCompletedJobs :one
SELECT COUNT(*) FROM job_queue WHERE queue_name = $1 AND completed_at IS NOT NULL;

-- name: CountDLQJobs :one
SELECT COUNT(*) FROM job_queue_dlq WHERE queue_name = $1;

-- name: SweepStuckJobs :many
-- Find jobs that exhausted retries but were never completed or moved to DLQ.
SELECT * FROM job_queue
WHERE queue_name = $1
  AND completed_at IS NULL
  AND retry_count >= max_retries;
