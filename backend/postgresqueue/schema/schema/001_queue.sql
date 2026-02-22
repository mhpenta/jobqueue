-- Job queue table (pending + completed jobs)
CREATE TABLE IF NOT EXISTS job_queue (
    id            TEXT PRIMARY KEY NOT NULL,
    queue_name    TEXT NOT NULL,
    job_type      TEXT NOT NULL,
    body          BYTEA NOT NULL,
    priority      INTEGER NOT NULL DEFAULT 0,
    visible_after BIGINT NOT NULL,  -- Unix timestamp: visible when now >= visible_after
    created_at    BIGINT NOT NULL,
    retry_count   INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 3,
    completed_at  BIGINT  -- NULL = pending, set = completed
);

-- Index for atomic dequeue: find visible pending jobs
CREATE INDEX IF NOT EXISTS idx_job_queue_dequeue
ON job_queue(queue_name, completed_at, visible_after, priority DESC, created_at ASC);

-- Index for listing completed jobs by most recent completion
CREATE INDEX IF NOT EXISTS idx_job_queue_completed
ON job_queue(queue_name, completed_at DESC);

-- Dead letter queue for failed jobs
CREATE TABLE IF NOT EXISTS job_queue_dlq (
    id          TEXT PRIMARY KEY NOT NULL,
    queue_name  TEXT NOT NULL,
    job_type    TEXT NOT NULL,
    body        BYTEA NOT NULL,
    priority    INTEGER NOT NULL,
    created_at  BIGINT NOT NULL,
    failed_at   BIGINT NOT NULL,
    retry_count INTEGER NOT NULL,
    error       TEXT NOT NULL
);

-- Index for viewing DLQ by queue
CREATE INDEX IF NOT EXISTS idx_job_queue_dlq_queue
ON job_queue_dlq(queue_name, failed_at DESC);

-- Readable, derived view for ad-hoc debugging/ops queries.
-- This does not change queue behavior; it only exposes computed columns.
CREATE OR REPLACE VIEW job_queue_readable AS
SELECT
    id,
    queue_name,
    job_type,
    priority,
    retry_count,
    max_retries,
    visible_after,
    to_timestamp(visible_after) AS visible_after_ts,
    created_at,
    to_timestamp(created_at) AS created_at_ts,
    completed_at,
    CASE
        WHEN completed_at IS NULL THEN NULL
        ELSE to_timestamp(completed_at)
    END AS completed_at_ts,
    octet_length(body) AS body_bytes,
    LEFT(encode(body, 'escape'), 240) AS body_preview,
    CASE
        WHEN completed_at IS NOT NULL THEN 'completed'
        WHEN retry_count >= max_retries THEN 'exhausted'
        WHEN visible_after > EXTRACT(EPOCH FROM NOW())::BIGINT THEN 'scheduled'
        ELSE 'pending'
    END AS derived_status
FROM job_queue;

-- Readable DLQ view for ad-hoc debugging/ops queries.
CREATE OR REPLACE VIEW job_queue_dlq_readable AS
SELECT
    id,
    queue_name,
    job_type,
    priority,
    retry_count,
    error,
    created_at,
    to_timestamp(created_at) AS created_at_ts,
    failed_at,
    to_timestamp(failed_at) AS failed_at_ts,
    octet_length(body) AS body_bytes,
    LEFT(encode(body, 'escape'), 240) AS body_preview
FROM job_queue_dlq;
