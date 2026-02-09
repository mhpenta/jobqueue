-- Job queue table (pending + completed jobs)
CREATE TABLE IF NOT EXISTS job_queue (
    id            TEXT PRIMARY KEY NOT NULL,
    queue_name    TEXT NOT NULL,
    job_type      TEXT NOT NULL,
    body          BLOB NOT NULL,
    priority      INTEGER NOT NULL DEFAULT 0,
    visible_after INTEGER NOT NULL,  -- Unix timestamp: visible when now >= visible_after
    created_at    INTEGER NOT NULL,
    retry_count   INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 3,
    completed_at  INTEGER  -- NULL = pending, set = completed
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
    body        BLOB NOT NULL,
    priority    INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    failed_at   INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    error       TEXT NOT NULL
);

-- Index for viewing DLQ by queue
CREATE INDEX IF NOT EXISTS idx_job_queue_dlq_queue
ON job_queue_dlq(queue_name, failed_at DESC);
