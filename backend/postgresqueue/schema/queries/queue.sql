-- name: InsertJob :one
INSERT INTO job_queue (
    id, job_key, queue_name, job_type, body, metadata, priority, visible_after, created_at, claimed_at, claimed_by, retry_count, max_retries, terminal_code, terminal_summary, result_json
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, '', 0, $10, '', '', '{}'::jsonb
)
RETURNING *;

-- name: InsertJobUnique :many
INSERT INTO job_queue (
    id, job_key, queue_name, job_type, body, metadata, priority, visible_after, created_at, claimed_at, claimed_by, retry_count, max_retries, terminal_code, terminal_summary, result_json
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, '', 0, $10, '', '', '{}'::jsonb
)
ON CONFLICT DO NOTHING
RETURNING *;

-- name: ClaimJob :one
-- Atomically claim the next available pending job.
-- A row with claimed_at set and visible_after > 0 was claimed earlier and its
-- lease ran out without Complete, Retry, or Fail (Retry clears claimed_at, and
-- releases written by older versions set visible_after to 0). Such an expired
-- claim counts as a failed attempt, so a job that kills or hangs its worker
-- cannot be redelivered forever.
UPDATE job_queue
SET visible_after = sqlc.arg(lease_until),
    claimed_at = sqlc.arg(claimed_at),
    claimed_by = sqlc.arg(claimed_by),
    retry_count = retry_count + CASE WHEN claimed_at IS NOT NULL AND visible_after > 0 THEN 1 ELSE 0 END
WHERE id = (
    SELECT jq.id FROM job_queue jq
    WHERE jq.queue_name = sqlc.arg(queue_name)
      AND jq.completed_at IS NULL
      AND jq.visible_after <= sqlc.arg(now)
      AND (
        CASE WHEN jq.claimed_at IS NOT NULL AND jq.visible_after > 0 THEN jq.retry_count + 1 ELSE jq.retry_count END < jq.max_retries
        OR (jq.max_retries = 0 AND jq.retry_count = 0 AND jq.claimed_at IS NULL)
      )
    ORDER BY jq.priority DESC, jq.created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: CompleteJob :execresult
UPDATE job_queue SET completed_at = $1 WHERE id = $2 AND completed_at IS NULL;

-- name: CompleteJobWithResult :execresult
UPDATE job_queue
SET completed_at = $1,
    terminal_code = $2,
    terminal_summary = $3,
    result_json = $4
WHERE id = $5 AND completed_at IS NULL;

-- name: ReleaseJob :execresult
-- Record a failed attempt and release the claim so the job can run again.
UPDATE job_queue
SET visible_after = sqlc.arg(visible_after),
    retry_count = sqlc.arg(retry_count),
    claimed_at = NULL
WHERE id = sqlc.arg(id) AND completed_at IS NULL;

-- name: GetJob :one
SELECT * FROM job_queue WHERE id = $1;

-- name: GetJobByKey :one
SELECT * FROM job_queue WHERE queue_name = $1 AND job_key = $2;

-- name: GetJobForUpdate :one
SELECT * FROM job_queue WHERE id = $1 FOR UPDATE;

-- name: InsertDLQ :exec
INSERT INTO job_queue_dlq (
    id, job_key, queue_name, job_type, body, metadata, priority, created_at, failed_at, claimed_at, claimed_by, retry_count, error, terminal_code, terminal_summary, result_json
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);

-- name: DeleteJob :exec
DELETE FROM job_queue WHERE id = $1;

-- name: GetDLQJob :one
SELECT * FROM job_queue_dlq WHERE id = $1;

-- name: GetDLQJobByKey :one
SELECT * FROM job_queue_dlq WHERE queue_name = $1 AND job_key = $2;

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
-- Find jobs that can never be claimed again: either their retry count is
-- exhausted, or their lease expired and counting that expiry exhausts it.
SELECT * FROM job_queue
WHERE queue_name = sqlc.arg(queue_name)
  AND completed_at IS NULL
  AND (
    (retry_count > 0 AND retry_count >= max_retries)
    OR (
      claimed_at IS NOT NULL
      AND visible_after > 0
      AND visible_after <= sqlc.arg(now)
      AND retry_count + 1 >= max_retries
    )
  );
