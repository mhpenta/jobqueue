-- Job queue table (pending + completed jobs)
CREATE TABLE IF NOT EXISTS job_queue (
    id            TEXT PRIMARY KEY NOT NULL,
    job_key       TEXT NOT NULL DEFAULT '',
    queue_name    TEXT NOT NULL,
    job_type      TEXT NOT NULL,
    body          BLOB NOT NULL,
    metadata      TEXT NOT NULL DEFAULT '{}',
    priority      INTEGER NOT NULL DEFAULT 0,
    visible_after INTEGER NOT NULL,  -- Unix timestamp: visible when now >= visible_after
    created_at    INTEGER NOT NULL,
    claimed_at    INTEGER,
    claimed_by    TEXT NOT NULL DEFAULT '',
    retry_count   INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 3,
    completed_at  INTEGER,  -- NULL = pending, set = completed
    terminal_code TEXT NOT NULL DEFAULT '',
    terminal_summary TEXT NOT NULL DEFAULT '',
    result_json   TEXT NOT NULL DEFAULT '{}'
);

-- Index for atomic dequeue: find visible pending jobs
CREATE INDEX IF NOT EXISTS idx_job_queue_dequeue
ON job_queue(queue_name, completed_at, visible_after, priority DESC, created_at ASC);

-- Index for listing completed jobs by most recent completion
CREATE INDEX IF NOT EXISTS idx_job_queue_completed
ON job_queue(queue_name, completed_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_queue_job_key
ON job_queue(queue_name, job_key)
WHERE job_key <> '';

-- Dead letter queue for failed jobs
CREATE TABLE IF NOT EXISTS job_queue_dlq (
    id          TEXT PRIMARY KEY NOT NULL,
    job_key     TEXT NOT NULL DEFAULT '',
    queue_name  TEXT NOT NULL,
    job_type    TEXT NOT NULL,
    body        BLOB NOT NULL,
    metadata    TEXT NOT NULL DEFAULT '{}',
    priority    INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    failed_at   INTEGER NOT NULL,
    claimed_at  INTEGER,
    claimed_by  TEXT NOT NULL DEFAULT '',
    retry_count INTEGER NOT NULL,
    error       TEXT NOT NULL,
    terminal_code TEXT NOT NULL DEFAULT '',
    terminal_summary TEXT NOT NULL DEFAULT '',
    result_json TEXT NOT NULL DEFAULT '{}'
);

-- Index for viewing DLQ by queue
CREATE INDEX IF NOT EXISTS idx_job_queue_dlq_queue
ON job_queue_dlq(queue_name, failed_at DESC);

-- Readable, derived view for ad-hoc debugging/ops queries.
-- This does not change queue behavior; it only exposes computed columns.
CREATE VIEW IF NOT EXISTS job_queue_readable AS
SELECT
    id,
    job_key,
    queue_name,
    job_type,
    priority,
    retry_count,
    max_retries,
    claimed_at,
    CASE
        WHEN claimed_at IS NULL THEN NULL
        ELSE datetime(claimed_at, 'unixepoch')
    END AS claimed_at_ts,
    claimed_by,
    visible_after,
    datetime(visible_after, 'unixepoch') AS visible_after_ts,
    created_at,
    datetime(created_at, 'unixepoch') AS created_at_ts,
    completed_at,
    CASE
        WHEN completed_at IS NULL THEN NULL
        ELSE datetime(completed_at, 'unixepoch')
    END AS completed_at_ts,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
    length(body) AS body_bytes,
    substr(hex(body), 1, 240) AS body_preview_hex,
    CASE
        WHEN completed_at IS NOT NULL THEN 'completed'
        WHEN retry_count >= max_retries THEN 'exhausted'
        WHEN visible_after > CAST(strftime('%s', 'now') AS INTEGER) THEN 'scheduled'
        ELSE 'pending'
    END AS derived_status
FROM job_queue;

-- Readable DLQ view for ad-hoc debugging/ops queries.
CREATE VIEW IF NOT EXISTS job_queue_dlq_readable AS
SELECT
    id,
    job_key,
    queue_name,
    job_type,
    priority,
    retry_count,
    error,
    claimed_at,
    CASE
        WHEN claimed_at IS NULL THEN NULL
        ELSE datetime(claimed_at, 'unixepoch')
    END AS claimed_at_ts,
    claimed_by,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
    created_at,
    datetime(created_at, 'unixepoch') AS created_at_ts,
    failed_at,
    datetime(failed_at, 'unixepoch') AS failed_at_ts,
    length(body) AS body_bytes,
    substr(hex(body), 1, 240) AS body_preview_hex
FROM job_queue_dlq;

CREATE VIEW IF NOT EXISTS job_queue_all_readable AS
SELECT
    'queue' AS source_table,
    id,
    job_key,
    queue_name,
    job_type,
    priority,
    retry_count,
    max_retries,
    claimed_at,
    datetime(claimed_at, 'unixepoch') AS claimed_at_ts,
    claimed_by,
    created_at,
    datetime(created_at, 'unixepoch') AS created_at_ts,
    completed_at,
    datetime(completed_at, 'unixepoch') AS completed_at_ts,
    NULL AS failed_at,
    NULL AS failed_at_ts,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
    substr(hex(body), 1, 240) AS body_preview_hex
FROM job_queue
UNION ALL
SELECT
    'dlq' AS source_table,
    id,
    job_key,
    queue_name,
    job_type,
    priority,
    retry_count,
    NULL AS max_retries,
    claimed_at,
    datetime(claimed_at, 'unixepoch') AS claimed_at_ts,
    claimed_by,
    created_at,
    datetime(created_at, 'unixepoch') AS created_at_ts,
    NULL AS completed_at,
    NULL AS completed_at_ts,
    failed_at,
    datetime(failed_at, 'unixepoch') AS failed_at_ts,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
    substr(hex(body), 1, 240) AS body_preview_hex
FROM job_queue_dlq;
