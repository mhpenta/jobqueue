-- Job queue table (pending + completed jobs)
CREATE TABLE IF NOT EXISTS job_queue (
    id            TEXT PRIMARY KEY NOT NULL,
    job_key       TEXT NOT NULL DEFAULT '',
    queue_name    TEXT NOT NULL,
    job_type      TEXT NOT NULL,
    body          BYTEA NOT NULL,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority      INTEGER NOT NULL DEFAULT 0,
    visible_after BIGINT NOT NULL,  -- Unix timestamp: visible when now >= visible_after
    created_at    BIGINT NOT NULL,
    claimed_at    BIGINT,
    claimed_by    TEXT NOT NULL DEFAULT '',
    retry_count   INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 3,
    completed_at  BIGINT,  -- NULL = pending, set = completed
    terminal_code TEXT NOT NULL DEFAULT '',
    terminal_summary TEXT NOT NULL DEFAULT '',
    result_json   JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS job_key TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS claimed_at BIGINT;
ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS claimed_by TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS terminal_code TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS terminal_summary TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS result_json JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Index for atomic dequeue: find visible pending jobs
CREATE INDEX IF NOT EXISTS idx_job_queue_dequeue
ON job_queue(queue_name, completed_at, visible_after, priority DESC, created_at ASC);

-- Index for listing completed jobs by most recent completion
CREATE INDEX IF NOT EXISTS idx_job_queue_completed
ON job_queue(queue_name, completed_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_queue_job_key
ON job_queue(queue_name, job_key)
WHERE job_key <> '';

CREATE INDEX IF NOT EXISTS idx_job_queue_metadata
ON job_queue USING GIN (metadata);

-- Dead letter queue for failed jobs
CREATE TABLE IF NOT EXISTS job_queue_dlq (
    id          TEXT PRIMARY KEY NOT NULL,
    job_key     TEXT NOT NULL DEFAULT '',
    queue_name  TEXT NOT NULL,
    job_type    TEXT NOT NULL,
    body        BYTEA NOT NULL,
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority    INTEGER NOT NULL,
    created_at  BIGINT NOT NULL,
    failed_at   BIGINT NOT NULL,
    claimed_at  BIGINT,
    claimed_by  TEXT NOT NULL DEFAULT '',
    retry_count INTEGER NOT NULL,
    error       TEXT NOT NULL,
    terminal_code TEXT NOT NULL DEFAULT '',
    terminal_summary TEXT NOT NULL DEFAULT '',
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS job_key TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS claimed_at BIGINT;
ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS claimed_by TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS terminal_code TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS terminal_summary TEXT NOT NULL DEFAULT '';
ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS result_json JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Index for viewing DLQ by queue
CREATE INDEX IF NOT EXISTS idx_job_queue_dlq_queue
ON job_queue_dlq(queue_name, failed_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_queue_dlq_metadata
ON job_queue_dlq USING GIN (metadata);

-- Readable, derived view for ad-hoc debugging/ops queries.
-- This does not change queue behavior; it only exposes computed columns.
CREATE OR REPLACE VIEW job_queue_readable AS
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
        ELSE to_timestamp(claimed_at)
    END AS claimed_at_ts,
    claimed_by,
    visible_after,
    to_timestamp(visible_after) AS visible_after_ts,
    created_at,
    to_timestamp(created_at) AS created_at_ts,
    completed_at,
    CASE
        WHEN completed_at IS NULL THEN NULL
        ELSE to_timestamp(completed_at)
    END AS completed_at_ts,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
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
    job_key,
    queue_name,
    job_type,
    priority,
    retry_count,
    error,
    claimed_at,
    CASE
        WHEN claimed_at IS NULL THEN NULL
        ELSE to_timestamp(claimed_at)
    END AS claimed_at_ts,
    claimed_by,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
    created_at,
    to_timestamp(created_at) AS created_at_ts,
    failed_at,
    to_timestamp(failed_at) AS failed_at_ts,
    octet_length(body) AS body_bytes,
    LEFT(encode(body, 'escape'), 240) AS body_preview
FROM job_queue_dlq;

CREATE OR REPLACE VIEW job_queue_all_readable AS
SELECT
    'queue'::text AS source_table,
    id,
    job_key,
    queue_name,
    job_type,
    priority,
    retry_count,
    max_retries,
    claimed_at,
    to_timestamp(claimed_at) AS claimed_at_ts,
    claimed_by,
    created_at,
    to_timestamp(created_at) AS created_at_ts,
    completed_at,
    to_timestamp(completed_at) AS completed_at_ts,
    NULL::bigint AS failed_at,
    NULL::timestamptz AS failed_at_ts,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
    LEFT(encode(body, 'escape'), 240) AS body_preview
FROM job_queue
UNION ALL
SELECT
    'dlq'::text AS source_table,
    id,
    job_key,
    queue_name,
    job_type,
    priority,
    retry_count,
    NULL::integer AS max_retries,
    claimed_at,
    to_timestamp(claimed_at) AS claimed_at_ts,
    claimed_by,
    created_at,
    to_timestamp(created_at) AS created_at_ts,
    NULL::bigint AS completed_at,
    NULL::timestamptz AS completed_at_ts,
    failed_at,
    to_timestamp(failed_at) AS failed_at_ts,
    terminal_code,
    terminal_summary,
    metadata,
    result_json,
    LEFT(encode(body, 'escape'), 240) AS body_preview
FROM job_queue_dlq;
